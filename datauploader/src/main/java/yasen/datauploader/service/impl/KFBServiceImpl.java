package yasen.datauploader.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import yasen.datauploader.config.ConfigFactory;
import yasen.datauploader.config.UploaderConfiguration;
import yasen.datauploader.consts.SysConsts;
import yasen.datauploader.factory.ElasticSearchServiceFactory;
import yasen.datauploader.factory.HbaseServiceFactory;
import yasen.datauploader.factory.HdfsServiceFactory;
import yasen.datauploader.service.ElasticSearchService;
import yasen.datauploader.service.HbaseService;
import yasen.datauploader.service.HdfsService;
import yasen.datauploader.service.KFBService;
import yasen.datauploader.tool.DataUploaderTool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KFBServiceImpl implements KFBService {
    static Logger logger = Logger.getLogger(KFBServiceImpl.class.getName());

    UploaderConfiguration uploaderConf = null;
    ElasticSearchService elasticSearchService = null;
    Configuration hdfsConf = ConfigFactory.getHdfsConfiguration();
    HbaseService hbaseService = null;
    HdfsService hdfsService = null;

    public KFBServiceImpl(){
        elasticSearchService = ElasticSearchServiceFactory.getElasticSearchService();
        hbaseService = HbaseServiceFactory.getHbaseService();
        hdfsService = HdfsServiceFactory.getHdfsService();
        uploaderConf = new UploaderConfiguration();
    }

    @Override
    public int uploadKFB(String kfbDir,String institution) throws IOException {
        if(!validateDir(kfbDir))
            return 0;
        List<String> kfbs = listKfbFile(kfbDir);
        for(String e : kfbs){
            boolean success = true;
            File file = new File(e);
            String filename = file.getName();
            String barcode = filename.split("-")[0];
            String rowkey = DataUploaderTool.generateRandonNumber(3)+institution+barcode;
            String createDate = filename.split("-")[1].split("\\.")[0];


            //yasen/bigdata/kfb/year/mongth/day/姓名_医院_patientid.kfb
            String entryDate = DataUploaderTool.getTodayDate();
            String dirPrefixKfb = uploaderConf.getDirPrefixKfb();
            String datePath = DataUploaderTool.parseDateToPath(createDate);
            String hdfspath = dirPrefixKfb+datePath+SysConsts.LEFT_SLASH+filename;

            Map<String,String> meta = new HashMap<String,String>();
            meta.put(SysConsts.ROWKEY,rowkey);
            meta.put(SysConsts.CREATE_DATE,createDate);
            meta.put(SysConsts.ENTRYDATE,entryDate);
            meta.put(SysConsts.BARCODE,barcode);
            meta.put(SysConsts.HDFSPATH,hdfspath);

            success = SysConsts.FAILED != hbaseService.putOne(uploaderConf.getKfbTablename(),uploaderConf.getKfbCf(),meta);

            if(success) {
                success = SysConsts.FAILED != hdfsService.upFile(e, hdfspath, hdfsConf);
            }

            if(success){
                success = SysConsts.FAILED != elasticSearchService.insertOne(uploaderConf.getIndexKfb(),uploaderConf.getTypeKfb(),null,meta);
            }

            if(success){
                DataUploaderTool.recordLog(logger,"",false,"kfb上传成功");
            }else {
                DataUploaderTool.recordLog(logger, "", false, "kfb hbase与hdfs上传成功,ES上传失败");
            }
        }
        return 0;
    }

    public static List<String> listKfbFile(String dirPath){
        List<String> fileList = new LinkedList<>();
        File file = new File(dirPath);
        listFilehelp(file,fileList);
        return fileList;
    }
    private static void listFilehelp(File file,List<String> fileList){
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for(File e : files) {
                if(e.isFile()&&e.getName().endsWith("kfb")){
                    fileList.add(e.getParentFile().getAbsolutePath());
                }
            }
        }else if(file.isFile()&&file.getName().endsWith("kfb")){
            fileList.add(file.getParentFile().getAbsolutePath());
        }
    }
    private boolean validateDir(String path){
        File file = new File(path);
        if(file.exists()&&file.isDirectory()){
            return true;
        }
        return false;
    }
}
