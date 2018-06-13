package qed.datauploader.sinker;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import qed.datauploader.config.ConfigFactory;
import qed.datauploader.factory.HdfsServiceFactory;
import qed.datauploader.service.ElasticSearchService;
import qed.datauploader.service.HbaseService;
import qed.datauploader.service.HdfsService;
import qed.datauploader.config.IndexFromHBaseConfig;
import qed.datauploader.config.UploaderConfiguration;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.factory.ElasticSearchServiceFactory;
import qed.datauploader.factory.HbaseServiceFactory;
import qed.datauploader.tool.DataUploaderTool;
import qed.datauploader.tool.IMAParseTool;
import qed.datauploader.tool.Statistics;
import yasen.datauploader.tool.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * 这个上传类用于上传dicom文件
 */

public class UploaderOnlyThumbnail implements Runnable{

    static Logger logger = Logger.getLogger(Uploader.class.getName());

    UploaderConfiguration uploaderConf = null;
    IndexFromHBaseConfig indexFromHBaseConfig = null;
    Configuration hdfsConf = null;
    String name = null;
    ElasticSearchService elasticSearchService;
    HbaseService hbaseService;
    HdfsService hdfsService;

    List<String> list = null;
    public UploaderOnlyThumbnail(){}
    public UploaderOnlyThumbnail(List<String> list){
        this.list = list;
        uploaderConf = new UploaderConfiguration();
        indexFromHBaseConfig = new IndexFromHBaseConfig();
        hbaseService = HbaseServiceFactory.getHbaseService();
        hdfsService = HdfsServiceFactory.getHdfsService();
        elasticSearchService = ElasticSearchServiceFactory.getElasticSearchService();
        hdfsConf = ConfigFactory.getHdfsConfiguration();
    }

    @Override
    public void run() {
        if(list != null && list.size() != 0) {
            name = Thread.currentThread().getName();
            System.out.println(name+" is start，任务："+list.size());
            uploadBatch(list);
        }
    }

    private void uploadBatch(List<String> dicomDirList){

        int count = 0;
        for(String dir : dicomDirList) {
            count++;
            System.out.println(name+"开始解析并上传：" + count + dir);
            logger.log(Level.INFO,"开始解析并上传：" + count + dir);
            boolean success = true;

            //步骤一：判断文件是否损坏以及类型，dicom中图片是一张还是多张
            /**检查是否有损坏的dicom文件或者有dicom文件丢失，并返回总张数
             * numberOfSlices=0:多张图片在同一个IMA文件中，将格外处理，暂时算作错误
             * numberOfSlices=-1：有丢失
             * numberOfSlices>0:正常，接着处理
             * */

            File seriesDir = new File(dir);
            Map<Integer,String> seq2nameMap  = new HashMap<Integer, String>();
            int numberOfSlices = isExistBadFileOrLost(seriesDir,seq2nameMap);

            if (numberOfSlices == -1) {  //文件丢失或坏
                DataUploaderTool.recordLog(logger,dir, false, "存在损坏文件或者丢失");
                Statistics.fail++;
                logger.log(Level.ERROR,"存在损坏文件或者丢失:"  + dir);
                System.out.println("存在损坏文件或者丢失:" + dir);
                continue;
            }else if(numberOfSlices == 0){ //多个照片在同一个IMA文件中
                logger.log(Level.INFO,"特殊文件，多个照片在同一个IMA文件中，须格外处理:" + dir);
                System.out.println("特殊文件，多个照片在同一个IMA文件中，须格外处理:" + dir);
//                uploadSpecial(dir);
                continue;
            }
            logger.log(Level.INFO,"步骤一：图片完整性检查完成，张数：" + numberOfSlices);

            //步骤二：提取元信息
            JSONObject metaMsg = parseMeta(seriesDir);

            String SeriesUID = metaMsg.getString(SysConsts.SeriesInstanceUID_TAG).replace(".", "x");

            List<String> rowkeys = hbaseService.searchRowkeyByQualify(uploaderConf.getDicomTablename(), uploaderConf.getDicomCf(),
                    SysConsts.SeriesUID, SeriesUID);


            String rowkey = rowkeys.size() >0 ?rowkeys.get(0):null;

            if(rowkey == null){
                logger.log(Level.INFO,"步骤二：未查询到rowkey,：" + rowkey+"\t SeriesUID"+SeriesUID+"\t目录:"+dir);
                continue;
            }
            logger.log(Level.INFO,"步骤二：查询到rowkey,：" + rowkey+"\t SeriesUID"+SeriesUID);
            //构造扫描jpg
            String parent = seriesDir.getParentFile().getAbsolutePath();
            String fatherpath = parent + File.separator+"jpg";

            logger.log(Level.INFO,"步骤三：jpg目录,：" +fatherpath+", jpg目录下图片张数："+new File(fatherpath).listFiles().length);
            System.out.println(fatherpath);

            //这个map中键值：<dcm中缩略图rowkey：jpeg图片绝对路径>
            Map<String,String> map = new HashMap<String,String>();
            for(Map.Entry<Integer,String> entry : seq2nameMap.entrySet()){
                String value = entry.getValue();
                String jpgFilePath = fatherpath+File.separator + value+".jpeg";
                String seqNo = DataUploaderTool.formatDigitalToNBit(entry.getKey()+"", 6);
                map.put(rowkey+seqNo,jpgFilePath);
            }

            int successNumber = 0;
            for(Map.Entry<String,String> entry: map.entrySet()){
                String thumbnailrowkey = entry.getKey();
                String jpgPath = entry.getValue();
                byte[] data = getByteArray(jpgPath);
                if(data != null){
                    try {
                        hbaseService.putCell(uploaderConf.getDicomThumbnailTablename(),thumbnailrowkey,
                                uploaderConf.getDicomThumbnailCf(),SysConsts.THUMBNAIL,data);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(data.length);
                    successNumber++;
                }else{
                    logger.log(Level.INFO,"步骤二：读取图片字节数组失败,：" + rowkey+"\t SeriesUID"+SeriesUID+"\t目录:"+dir);
                }
            }
            logger.log(Level.INFO,"步骤四：上传成功：" +successNumber);
        }
    }

    private byte[] getByteArray(String path){
        FileInputStream fin = null;
        byte[] data = null;
        try {
            fin = new FileInputStream(new File(path));
            int available = fin.available();
            data = new byte[available];
            fin.read(data);
            fin.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * 此处对元数据格式化，主要是格式化日期为yyyyMMdd，其他字段全部保留为字符串类型
     * @param metaMsg
     */
    private void doFormatMeta(JSONObject metaMsg) {

        /******************处理空值*********************/
        String sex = metaMsg.getString(SysConsts.PatientSex_TAG);
        if(StringUtils.isBlank(sex))
            metaMsg.put(SysConsts.PatientSex_TAG,"U");
        String seriesDate = metaMsg.getString(SysConsts.SeriesDate_TAG);
        if(StringUtils.isBlank(seriesDate)){
            metaMsg.put(SysConsts.SeriesDate_TAG,metaMsg.getString(SysConsts.StudyDate_TAG));
        }
        String studyDate = metaMsg.getString(SysConsts.StudyDate_TAG);
        if(StringUtils.isBlank(studyDate)){
            metaMsg.put(SysConsts.StudyDate_TAG,metaMsg.getString(SysConsts.SeriesDate_TAG));
        }

        /******************增添某些非标准字段***********************/
        metaMsg.put(SysConsts.ENTRYDATE,DataUploaderTool.getDateOfToday());
        metaMsg.put(SysConsts.PatientAge_TAG,
                DataUploaderTool.getPatientAge(metaMsg.getString(SysConsts.SeriesDate_TAG), metaMsg.getString(SysConsts.PatientBirthDate_TAG)));
        String patientMD5 = DataUploaderTool.getMD5(metaMsg.getString(SysConsts.PatientID_TAG)
                +metaMsg.getString(SysConsts.PatientName_TAG)
                +metaMsg.getString(SysConsts.PatientAge_TAG)
                +metaMsg.getString(SysConsts.PatientSex_TAG));

        metaMsg.put(SysConsts.PatientUID,patientMD5);
        metaMsg.put(SysConsts.SeriesUID, metaMsg.getString(SysConsts.SeriesInstanceUID_TAG).replace(".","x"));

        /******************格式化数据类型***********************/
        Set<String> keySet = metaMsg.keySet();
        for(String key : keySet){
            if(SysConsts.DICOM_META_TAGS.contains(key)){
                if(SysConsts.DCM_META_TAG2KW.get(key).endsWith("Date")){
                    metaMsg.replace(key,DataUploaderTool.formatDate(metaMsg.getString(key)));
                }
            }
        }
        metaMsg.replace(SysConsts.ENTRYDATE,DataUploaderTool.formatDate(metaMsg.getString(SysConsts.ENTRYDATE)));
    }

    private JSONObject convertTagToKeyword(JSONObject metaMsg){
        JSONObject result = new JSONObject();
        Set<String> keySet = metaMsg.keySet();
        for(String key : keySet){
            if(DataUploaderTool.isTag(key)){
                result.put(SysConsts.DCM_META_TAG2KW.get(key),metaMsg.get(key));
            }else{
                result.put(key,metaMsg.get(key));
            }
        }
        return result;

    }



    /**
     * 检查是否存在坏文件或者有文件丢失
     * 检查一个序列目录下是否有损坏的dicom文件
     * 方法为依次解析所有文件，如有损坏则返回false
     * 并且检查完好的dicom文件数量与应该存在dicom文件数量，如果数量对不上返回false.
     * @param seriesDir
     * @return
     */
    public int isExistBadFileOrLost(File seriesDir,Map<Integer,String> map) {

        int sum = -1;
        List<Integer> list = new ArrayList<Integer>();
        for(File e : seriesDir.listFiles()){
            String path = e.getAbsolutePath();
            String name = e.getName();
            JSONObject field = IMAParseTool.extractExactField(path,SysConsts.InstanceNumber_TAG);
            if(!field.isEmpty()){
                int imageNumber = Integer.parseInt(field.getString(SysConsts.InstanceNumber_TAG));
                map.put(imageNumber,name);
                list.add(imageNumber);
            }
        }
        if(list.size()!=0){
            int code = isContinuity(list);
            if(code==1){//连续递增,常规处理
                sum = list.size();
            }else if(code==0) {//全部相同张数都为1,说明一个序列在同一张IMA文件中，格外处理
                return 0;
            }
        }
        return sum;  //-1为文件解析错误，或者有丢失
    }

    /**
     * 判断List中的数是否具有连续性，因为都是自然数，并且按1递增，所以使用最大数最小数差值加一与总个数比较。
     * @param list
     * @return
     */
    private int isContinuity(List<Integer> list){
        Collections.sort(list);
        int first = list.get(0);
        int last = list.get(list.size()-1);
        if((last-first+1)==list.size()){
            return 1; //连续递增
        }else if((last-first)==0){
            return 0;  //全部相同张数都为1,格外处理
        }
        return -1;
    }

    //拷贝文件到本地
    public void copyFromHdfs(String sourceDir,String desDir){

    }

    /**
     * 解析单个文件元数据
     * @param seriesDir
     * @return
     */
    public JSONObject parseMeta(File seriesDir){
        if(seriesDir!=null && seriesDir.listFiles().length!=0) {
            for(File e : seriesDir.listFiles()){
                if(!IMAParseTool.isBadDicom(e.getAbsolutePath())){
                    return IMAParseTool.extractMetaMsg(e.getAbsolutePath());
                }
            }
        }
        return null;
    }

    /**
     * 生成存放序列的hdfs目录,
     * @param metaMsg
     * @param conf
     * @return 仅仅是路径，不带协议域名
     */
    public String generateFileHdfsPosition(JSONObject metaMsg, UploaderConfiguration conf){
        String seriesDate = metaMsg.getString(SysConsts.SeriesDate_TAG);
        if(seriesDate == null){
            seriesDate = metaMsg.getString(SysConsts.StudyDate_TAG);
        }
        String datePath = DataUploaderTool.parseDateToPath(seriesDate);
        String dirPrefix =conf.getDirPrefixDicom();
        String filename = metaMsg.getString(SysConsts.SeriesUID);
        return dirPrefix+datePath+ SysConsts.LEFT_SLASH +filename;
    }



    /**
     * 保存序列元数据到elasticsearch
     * @param json
     * @return
     */
    public int storeMetaToES(JSONObject json){
        return elasticSearchService.insertOne(uploaderConf.getIndexDicom(),uploaderConf.getTypeDicom(),json.getString(SysConsts.SeriesUID),json);
    }

    /**
     * @param list
     * @return
     * @deprecated
     */
    public boolean storeBatchToES(List<JSONObject> list){
//        ESTool.storePatchToES(list);
        return true;
    }

    /**
     * 阶段性步骤写入日志文件
     * @param seriesDir
     * @param isSuccess
     * @param msg
     */



    /**
     * 设计删除已经上传了的本地文件，暂时不删除。
     * @param json
     */
    public void deleteFile(JsonObject json){
//        File file = new File(json.get(SysConsts).toString());
//        file.deleteOnExit();
    }

    /**
     * 删除hdfs上的文件
     * @param path
     * @param hdfsConf
     */
    private static void deleteFileOnHdfs(String path, Configuration hdfsConf){
        try {
            FileSystem fs = FileSystem.get(hdfsConf);
            fs.deleteOnExit(new Path(path));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /********************************下面是特殊dicom文件（多张图片在同一个dicom文件中）**************************************************/

    /**
     * 上传特殊序列的方法，此处特殊指多个dicom文件在同一个IMA文件中
     * @param seriesDir
     * @return
     */
    public boolean uploadSpecial(String seriesDir){
        //验证是不是所谓的特殊文件
        if(verifySpecialDicom(seriesDir)){
            File dir = new File(seriesDir);
            for(File file : dir.listFiles()){
                JSONObject metaMsg = null;
                if(!IMAParseTool.isBadDicom(file.getAbsolutePath())) {
                    boolean success = true;
                    metaMsg = IMAParseTool.extractMetaMsg(file.getAbsolutePath());
//                        System.out.println(metaMsg.getString(SysConsts.SeriesUID));
                    JSONObject numberOfFrameField = IMAParseTool.extractExactField(file.getAbsolutePath(),
                            SysConsts.NumberOfFrames_TAG);
                    int numberOfSlices = Integer.parseInt(numberOfFrameField.getString(SysConsts.NumberOfFrames_TAG));
                    metaMsg.put(SysConsts.NumberOfSlices_TAG, numberOfSlices);
                    doFormatMeta(metaMsg);
                    String position = generateFileHdfsPosition(metaMsg, uploaderConf);
                    metaMsg.put(SysConsts.HDFSPATH, position);

                    //生成缩略图，并存储
                    //hbase dicom序列主表 rowkey:3位盐值+4位检查+16位CRC32(seriesUID)+8位CRC32(时间戳)：共31位
                    //缩略图rowkey为主表rowkey+6位图片序号
                    String seriesMetaRowkey = DataUploaderTool.generateRandonNumber(3)
                            + DataUploaderTool.formatDigitalToNBit(metaMsg.getString(SysConsts.StudyID_TAG),4)
                            + DataUploaderTool.getMD5(metaMsg.getString(SysConsts.SeriesUID)).substring(0,16)
                            + DataUploaderTool.getCRC32(DataUploaderTool.getTimeStamp() + "", 8);
                    metaMsg.put(SysConsts.ROWKEY, seriesMetaRowkey);
                    /**存储元数据到hbase，失败回滚**/

                    try {
                        success = SysConsts.SUCCESS == hbaseService.putOne(uploaderConf.getDicomTablename(), uploaderConf.getDicomCf(), metaMsg);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    /**存储缩略图到hbase，暂时还不能提取特殊文件里面的缩略图，他们的缩略图暂时不上传**/
                    if (success) {
//                        success = SysConsts.SUCCESS == hbaseService.uploadThumbnail(dir, metaMsg);
                    } else {
                        //回滚主表元数据
                        hbaseService.delete(uploaderConf.getDicomTablename(), metaMsg.getString(SysConsts.ROWKEY));
                    }

                    //存储数据到hdfs，失败回滚
                    if (success) {
                        success = SysConsts.SUCCESS == hdfsService.upCommonDicomToHdfs(file, position, hdfsConf);
                    } else {
                        //此处处理上传到hdfs之前的步骤回滚

                    }

                    if (success) {
                        success = SysConsts.SUCCESS == elasticSearchService.insertOne(uploaderConf.getIndexDicom(),
                                uploaderConf.getTypeDicom(),metaMsg.getString(SysConsts.SeriesUID),metaMsg);
                    }

                    //到达此步骤，说明全部成功，写该序列全部成功的日志
                    if(success){
                        System.out.println(success);
                        DataUploaderTool.recordLog(logger,seriesDir, true, "全部上传成功");
                    }else{
                        System.out.println(success);
                        DataUploaderTool.recordLog(logger,file.getAbsolutePath(), true, "hbase,hdfs上传成功");
                        DataUploaderTool.recordLog(logger,file.getAbsolutePath(), false, "ES上传失败");
                    }
                }
            }
        }
        return true;
    }

    /************传入一个IMA文件目录，验证里面文件是不是存放的特殊dicom文件，如果是返回true****************/
    private boolean verifySpecialDicom(String dir){
        File file = new File(dir);
        return isExistBadFileOrLost(file,null)==0;
    }
}
