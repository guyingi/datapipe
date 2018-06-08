package yasen.datauploader.service.impl;

import org.apache.hadoop.conf.Configuration;
import yasen.datauploader.config.ConfigFactory;
import yasen.datauploader.config.UploaderConfiguration;
import yasen.datauploader.consts.SysConsts;
import yasen.datauploader.factory.ElasticSearchServiceFactory;
import yasen.datauploader.factory.HbaseServiceFactory;
import yasen.datauploader.factory.HdfsServiceFactory;
import yasen.datauploader.service.DesensitizationService;
import yasen.datauploader.service.ElasticSearchService;
import yasen.datauploader.service.HbaseService;
import yasen.datauploader.service.HdfsService;
import yasen.datauploader.tool.DataUploaderTool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DesensitizationServiceImpl implements DesensitizationService {
    ElasticSearchService elasticSearchService = null;
    UploaderConfiguration uploaderConf = null;
    Configuration hdfsConf = ConfigFactory.getHdfsConfiguration();
    HbaseService hbaseService = null;
    HdfsService hdfsService = null;

    public DesensitizationServiceImpl(){
        elasticSearchService = ElasticSearchServiceFactory.getElasticSearchService();
        hbaseService = HbaseServiceFactory.getHbaseService();
        hdfsService = HdfsServiceFactory.getHdfsService();
        uploaderConf = new UploaderConfiguration();
    }

    @Override
    public int desensitization(String seriesUIDFile, String destination) {
        return 0;
    }

    @Override
    public int uploadDicomDesensitization(String desensitizationDir,String tag) throws IOException {
        if(!validateDir(desensitizationDir))
            return SysConsts.FAILED;
        List<File> seriesDirs = listDir(desensitizationDir);
        for(File seriesDir : seriesDirs){
            //逐个上传
            uploadDicomDesensitization(seriesDir,tag);
        }
        return 0;
    }

    private int uploadDicomDesensitization(File seriesDir,String tag) throws IOException {
        boolean success = true;

        /**步骤1：先获取SeriesUID,即目录名称。*/
        String seriesUID = seriesDir.getName();

        /**步骤2：查询ES，获取必要数据:【PatientUID】,【StudyID】【rowkey】,
         * 沿用dicom序列rowkey：rowkey:3位盐值+4位检查+MD5(seriesUID).sub(0,16)+CRC32(时间戳)
         **/
        String patientUID = (String)elasticSearchService.getField(uploaderConf.getIndexDicom(), uploaderConf.getTypeDicom(), seriesUID, SysConsts.PatientUID);
        String studyID = (String)elasticSearchService.getField(uploaderConf.getIndexDicom(), uploaderConf.getTypeDicom(),
                seriesUID, SysConsts.DCM_META_TAG2KW.get(SysConsts.StudyID_TAG));
        String rowkey = (String)elasticSearchService.getField(uploaderConf.getIndexDicom(), uploaderConf.getTypeDicom(), seriesUID, SysConsts.ROWKEY);

        /**
         * 步骤3：目录/yasen/bigdata/raw/tag/year/month/day/seriesUID下面存放raw+mhd文件
         */
        String entryDate  = DataUploaderTool.getTodayDate();
        String dirPrefixDesensitization = uploaderConf.getDirPrefixDesensitization();
        String datePath = DataUploaderTool.parseDateToPath(entryDate);
        String hdfspath = dirPrefixDesensitization+SysConsts.LEFT_SLASH+tag+datePath+SysConsts.LEFT_SLASH+seriesUID;

        Map<String,String> metaData = new HashMap<String,String>();
        metaData.put(SysConsts.ROWKEY,rowkey);
        metaData.put(SysConsts.PatientUID,patientUID);
        metaData.put(SysConsts.SeriesUID,seriesUID);
        metaData.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.StudyID_TAG),studyID);
        metaData.put(SysConsts.HDFSPATH,hdfspath);
        metaData.put(SysConsts.ENTRYDATE,entryDate);
        metaData.put(SysConsts.TAG,tag);

        for(Map.Entry<String,String> entry : metaData.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }

        /**步骤4：写入hbase表格中 */
        success = SysConsts.FAILED != hbaseService.putOne(uploaderConf.getDicomDisensitizationTablename(),
                uploaderConf.getDicomDisensitizationCf(), metaData);

        System.out.println("hbase结果："+success);
        /**步骤5：缩略图暂时不做 */

        /**步骤6：上传hdfs */
        if(success) {
            success = SysConsts.SUCCESS == hdfsService.upDicomDesensitization(seriesDir.getAbsolutePath(), hdfspath, hdfsConf);
        }else{
            hbaseService.delete(uploaderConf.getDicomDisensitizationTablename(),rowkey);
            return SysConsts.FAILED;
        }
        System.out.println("hdfs结果："+success);
        /**步骤6：写入es索引中*/
        if(success) {
            success = SysConsts.FAILED != elasticSearchService.insertOne(uploaderConf.getIndexDicomDisensitization(),
                    uploaderConf.getTypeDicomDisensitization(), seriesUID, metaData);
        }
        System.out.println("es结果："+success);
        return SysConsts.SUCCESS;
    }

    /**************下面是内部方法****************/
    private List<File> listDir(String path){
        File file = new File(path);
        List<File> dirs = new ArrayList<File>();
        for(File e : file.listFiles()){
            dirs.add(e);
        }
        return dirs;
    }

    private boolean validateDir(String path){
        File file = new File(path);
        if(file.exists()&&file.isDirectory()){
            return true;
        }
        return false;
    }
    private String generateFileHdfsPosition(){
        return null;
    }
}
