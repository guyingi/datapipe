package yasen.datauploader.sinker;

import com.alibaba.fastjson.JSONObject;
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
import yasen.datauploader.tool.DataUploaderTool;

import java.io.*;
import java.util.List;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.datauploader.sinker
 * @Description: ${todo}
 * @date 2018/5/29 14:03
 */
public class GaugetableUploader {

    static Logger logger = Logger.getLogger(ElectricSignalUploader.class.getName());
    UploaderConfiguration uploaderconf;
    Configuration hdfsconf = null;
    HdfsService hdfsService = null;
    HbaseService hbaseService = null;
    ElasticSearchService elasticSearchService = null;

    public GaugetableUploader(){
        init();
    }

    private void init(){
        uploaderconf = ConfigFactory.getUploaderConfiguration();
        hdfsconf = ConfigFactory.getHdfsConfiguration();
        hdfsService = HdfsServiceFactory.getHdfsService();
        hbaseService = HbaseServiceFactory.getHbaseService();
        elasticSearchService = ElasticSearchServiceFactory.getElasticSearchService();
    }

    public void upload(String dir) throws IOException {

        boolean success = true;
        //扫描目录得到所有qes文件，
        String suffix = "qes";
        List<String> gaugetablePaths = DataUploaderTool.listEdfPath(dir,suffix);
        if(gaugetablePaths != null && gaugetablePaths.size() != 0) {
            success = true;
        }

        //解析qes文件，得到患者姓名，出生年月，性别
        //患者名字	出生日期	性别	医院	创建日期	hbase元数据表rowkey	file
        parseGuagetable(gaugetablePaths);
        String patientName = "程登群";
        String patientuid = DataUploaderTool.getMD5(patientName);
        String birthday = "20121212";
        String sex = "F";
        Integer PatientsAge = 0;  //没有
        String institutionName = uploaderconf.getEdfHospital(); //医院，配置文件获取
        String createdate = "20180101";
        String entrydate = DataUploaderTool.getTodayDate();
        String rowkey = DataUploaderTool.generateRandonNumber(3)+patientuid;
        String hdfspath =  uploaderconf.getDirPrefixGuagetable()
                +DataUploaderTool.parseDateToPath(createdate)+SysConsts.LEFT_SLASH+patientuid;

        //生成meta元信息
        JSONObject meta = new JSONObject();
        meta.put(SysConsts.PatientUID,patientuid);
        meta.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientName_TAG),patientName);
        meta.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientAge_TAG),PatientsAge);
        meta.put(SysConsts.CREATE_DATE,createdate);
        meta.put(SysConsts.DCM_META_TAG2KW.get(SysConsts.InstitutionName_TAG),institutionName);
        meta.put(SysConsts.ENTRYDATE,entrydate);
        meta.put(SysConsts.ROWKEY,rowkey);
        meta.put(SysConsts.HDFSPATH,hdfspath);
        System.out.println(meta.toJSONString());

        //上传hdfs


        //上传Hbase

        //上传es


    }

    public static void main(String[] args) throws IOException {
        String str = "F:\\lab\\量表";
        new GaugetableUploader().upload(str);
    }

    public JSONObject parseGuagetable(List<String> gaugetablePaths) throws IOException {
        for(String e : gaugetablePaths){
            File file = new File(e);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),"GB2312"));
            String temp = null;
            int count  = 10;
            while((temp=br.readLine())!=null && count-->0){

                System.out.println(temp);
            }
        }
        return null;
    }
}
