package qed.datauploader.uploader;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import qed.datauploader.service.ElasticSearchService;
import qed.datauploader.service.HbaseService;
import qed.datauploader.config.ConfigFactory;
import qed.datauploader.config.UploaderConfiguration;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.factory.ElasticSearchServiceFactory;
import qed.datauploader.factory.HbaseServiceFactory;
import qed.datauploader.factory.HdfsServiceFactory;
import qed.datauploader.service.HdfsService;
import qed.datauploader.tool.DataUploaderTool;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.datauploader.uploader
 * @Description: ${todo}
 * @date 2018/5/29 9:42
 */
public class ElectricSignalUploader {

    static Logger logger = Logger.getLogger(ElectricSignalUploader.class.getName());

    UploaderConfiguration uploaderconf;
    Configuration hdfsconf = null;
    HdfsService hdfsService = null;
    HbaseService hbaseService = null;
    ElasticSearchService elasticSearchService = null;




    public ElectricSignalUploader(){
        init();
    }
    private void init(){
        uploaderconf = ConfigFactory.getUploaderConfiguration();
        hdfsconf = ConfigFactory.getHdfsConfiguration();
        hdfsService = HdfsServiceFactory.getHdfsService();
        hbaseService = HbaseServiceFactory.getHbaseService();
        elasticSearchService = ElasticSearchServiceFactory.getElasticSearchService();
    }

    public int upload(String dir) {
        boolean success = false;
        //扫描脑电目录，得到所有脑电文件路径列表，然后逐个上传
        String suffix = "edf";
        List<String> edfPaths = DataUploaderTool.listEdfPath(dir,suffix);
        if(edfPaths != null && edfPaths.size() != 0){
            success = true;
        }

        //循环上传每一个脑电edf文件
        if(success){
           for(String e : edfPaths){
               uploadSingle(e);
           }
        }


        return 0;
    }

    public boolean uploadSingle(String edfpath) {

        boolean success = true;
        //先生成元数据对象，其中包括hdfspath
        File file = new File(edfpath);

        String tempname = file.getName().split("\\.")[0];  //从文件名解析而来，001程登群.edf
        String PatientName = tempname.substring(3,tempname.length());
        //应该为姓名+生日+性别生成的md5,128位。  但是现在探索阶段只有姓名，那么就只使用姓名。
        String patientuid = DataUploaderTool.getMD5(PatientName);
        String rowkey = DataUploaderTool.generateRandonNumber(3)+patientuid;
        Integer PatientsAge = 0;  //没有
        String CreateDate = DataUploaderTool.getLastModifyDateOfFile(edfpath);   //采用edf文件的最后修改时间
        String InstitutionName = uploaderconf.getEdfHospital(); //医院，配置文件获取
        String entrydate = DataUploaderTool.getTodayDate();
        //配置文件获取目录前缀，CreateDate转化为/年/月/日/MD5(patientuid)_脑电原文件名.edf
        String hdfspath = uploaderconf.getDirPrefixEdf()
                +DataUploaderTool.parseDateToPath(CreateDate)+SysConsts.LEFT_SLASH+patientuid+SysConsts.LINE+file.getName();

        JSONObject meta = new JSONObject();
        meta.put(SysConsts.PatientUID,patientuid);
        meta.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientName_TAG),PatientName);
        meta.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientAge_TAG),PatientsAge);
        meta.put(SysConsts.CREATE_DATE,CreateDate);
        meta.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.InstitutionName_TAG),InstitutionName);
        meta.put(SysConsts.ENTRYDATE,entrydate);
        meta.put(SysConsts.ROWKEY,rowkey);
        meta.put(SysConsts.HDFSPATH,hdfspath);
        System.out.println(meta.toJSONString());

        //上传至hdfs,如果成功继续后面的工作
        try {
            if(SysConsts.FAILED == hdfsService.upFile(file.getAbsolutePath(),hdfspath,hdfsconf)){
                success = false;
            }
            System.out.println("上传hdfs成功");
        } catch (IOException e) {
            e.printStackTrace();
        }

        //将元素据写入hbase.  成功则继续，失败回滚
        if(success){
            try {
                if(SysConsts.FAILED == hbaseService.putOne(uploaderconf.getEdfTablename()
                        ,uploaderconf.getEdfCf(),meta)){
                    success = false;
                    if(false == hdfsService.del(meta.getString(SysConsts.HDFSPATH),hdfsconf)){
                        logger.log(org.apache.log4j.Level.ERROR,"删除hdfs上edf文件失败："+meta.getString(SysConsts.HDFSPATH));
                    }
                }
                System.out.println("上传hbase成功");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //写元数据到es,失败不回滚，严格记录日志。主要用于排错
        if(success){
            if(SysConsts.FAILED == elasticSearchService.insertOne(uploaderconf.getIndexEdf(),
                    uploaderconf.getTypeEdf(),null,meta)){
                success = false;
            }
            System.out.println("上传ES成功");
        }
        return true;
    }



    public static void main(String[] args) {
        String str = "F:\\实验室\\EEG";
        new ElectricSignalUploader().upload(str);
//        FileSystem fs;
//        try {
//            fs = FileSystem.get(hdfsconf);
//            fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(localFilePath),new org.apache.hadoop.fs.Path(remoteFilePath));
//        } catch (IOException e) {
//            e.printStackTrace();
//            return SysConsts.FAILED;
//        }
    }
}
