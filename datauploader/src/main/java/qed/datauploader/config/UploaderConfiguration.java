package qed.datauploader.config;

import org.apache.log4j.Logger;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.tool.DataUploaderTool;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class UploaderConfiguration {

    Logger logger = Logger.getLogger(UploaderConfiguration.class.getName());

    /**********临时目录名称********************/
    private String scriptdir = null;
    private String tempdir = null;
    private String dcm2jpgScriptPath = null;
    private String dcm2jpg_sh = null;
    private String dcm2jpg_bat = null;


    /**********hdfs目录前缀********************/
    private String defaultFS = null;
    private String dirPrefixDicom = null;
    private String dirPrefixDesensitization = null; //存放脱敏数据的目录前缀
    private String dirPrefixKfb = null; //存放kfb数据的目录前缀
    private String dirPrefixEdf = null; //存放edf数据的目录前缀
    private String dirPrefixGuagetable = null; //存放edf数据的目录前缀

    /**********es IP，端口********************/
    private String escluster = null;
    private String esip = null;
    private String eshost = null;
    private String esport = null;

    /**********es表********************/
    private String indexDicom = null;
    private String indexDicomDisensitization = null;
    private String indexKfb = null;
    private String indexEdf = null;
    private String indexGuagetable = null;
    private String typeDicom = null;
    private String typeDicomDisensitization = null;
    private String typeKfb = null;
    private String typeEdf = null;
    private String typeGuagetable = null;

    /**********跟数据相关参数********************/
    private String organ = null;
    private String edfHospital = null;

    private String zookeeperQuorum = null;
    private String zookeeperClientPort = null;

    /**********hbase表********************/
    private String dicomTablename = null;
    private String dicomSeriesuidTablename = null;
    private String dicomThumbnailTablename = null;
    private String dicomDisensitizationTablename = null;
    private String kfbTablename = null;
    private String edfTablename = null;
    private String guagetableTablename = null;
    private String dicomCf = null;
    private String dicomSeriesuidCf = null;
    private String dicomThumbnailCf = null;
    private String dicomDisensitizationCf = null;
    private String kfbCf = null;
    private String edfCf = null;
    private String guagetableCf = null;

    public UploaderConfiguration() {
        init();
    }

    public void init() {

        InputStreamReader reader = new InputStreamReader(UploaderConfiguration.class.getClassLoader().getResourceAsStream("datauploader.properties"));
        Properties props = new Properties();
        try {
            props.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        defaultFS = props.getProperty("fs.defaultFS");
        dirPrefixDicom = props.getProperty("dir.prefix.dicom");
        dirPrefixDesensitization = props.getProperty("dir.prefix.desensitization");
        dirPrefixKfb = props.getProperty("dir.prefix.kfb");
        dirPrefixEdf = props.getProperty("dir.prefix.edf");
        dirPrefixGuagetable = props.getProperty("dir.prefix.guagetable");

        escluster = props.getProperty("es.cluster");
        esip = props.getProperty("es.ip");
        eshost = props.getProperty("es.host");
        esport = props.getProperty("es.port");
        indexDicom = props.getProperty("es.dicom.index");
        indexDicomDisensitization = props.getProperty("es.disensitization.index");
        indexKfb = props.getProperty("es.kfb.index");
        indexEdf = props.getProperty("es.edf.index");
        indexGuagetable = props.getProperty("es.guagetable.index");

        typeDicom = props.getProperty("es.dicom.type");
        typeDicomDisensitization = props.getProperty("es.disensitization.type");
        typeKfb = props.getProperty("es.kfb.type");
        typeEdf = props.getProperty("es.edf.type");
        typeGuagetable = props.getProperty("es.guagetable.type");

        organ = props.getProperty("datauploader.organ");
        edfHospital = props.getProperty("edf.hospital");

        zookeeperQuorum = props.getProperty("hbase.zookeeper.quorum");
        zookeeperClientPort = props.getProperty("hbase.zookeeper.property.clientPort");

        dicomTablename = props.getProperty("dicom.tablename");
        dicomSeriesuidTablename = props.getProperty("dicom.seriesuid.tablename");
        dicomThumbnailTablename = props.getProperty("dicom.thumbnail.tablename");
        dicomDisensitizationTablename = props.getProperty("dicom.disensitization.tablename");
        kfbTablename = props.getProperty("kfb.tablename");
        edfTablename = props.getProperty("edf.tablename");
        guagetableTablename = props.getProperty("guagetable.tablename");


        dicomCf = props.getProperty("dicom.cf");
        dicomSeriesuidCf = props.getProperty("dicom.seriesuid.cf");
        dicomDisensitizationTablename = props.getProperty("dicom.disensitization.tablename");
        dicomThumbnailCf = props.getProperty("dicom.thumbnail.cf");
        dicomDisensitizationCf = props.getProperty("dicom.disensitization.cf");
        kfbCf = props.getProperty("kfb.cf");
        edfCf = props.getProperty("edf.cf");
        guagetableCf = props.getProperty("guagetable.cf");



        dcm2jpg_sh = props.getProperty("dcm2jpg_sh");
        dcm2jpg_bat = props.getProperty("dcm2jpg_bat");



        scriptdir = DataUploaderTool.getRunnerPath()+ File.separator+SysConsts.SCRIPT_DIRNAME;
        tempdir = DataUploaderTool.getRunnerPath()+ File.separator+SysConsts.TEMP_DIRNAME;
        if(SysConsts.WINDOWS.equals(DataUploaderTool.getOS())){
            dcm2jpgScriptPath = scriptdir+File.separator+dcm2jpg_bat;
        }else if(SysConsts.LINUX.equals(DataUploaderTool.getOS())){
            dcm2jpgScriptPath = scriptdir+File.separator+dcm2jpg_sh;
        }

        if(!new File(scriptdir).exists()){
            new File(scriptdir).mkdirs();
        }
        if(!new File(tempdir).exists()){
            new File(tempdir).mkdirs();
        }




    }

    public String getDefaultFS() {
        return defaultFS;
    }

    public String getDirPrefixDicom() {
        return dirPrefixDicom;
    }

    public String getDirPrefixDesensitization() {
        return dirPrefixDesensitization;
    }

    public String getDirPrefixEdf() {
        return dirPrefixEdf;
    }

    public String getEscluster() {
        return escluster;
    }

    public String getEsip() {
        return esip;
    }

    public String getEshost() {
        return eshost;
    }

    public String getEsport() {
        return esport;
    }

    public String getIndexDicom() {
        return indexDicom;
    }

    public String getIndexDicomDisensitization() {
        return indexDicomDisensitization;
    }

    public String getTypeDicom() {
        return typeDicom;
    }

    public String getTypeDicomDisensitization() {
        return typeDicomDisensitization;
    }

    public String getOrgan() {
        return organ;
    }

    public String getEdfHospital() {
        return edfHospital;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public String getZookeeperClientPort() { return zookeeperClientPort; }

    public String getDicomTablename() { return dicomTablename; }

    public String getDicomDisensitizationTablename() {
        return dicomDisensitizationTablename;
    }

    public String getDicomCf() { return dicomCf; }

    public String getDicomThumbnailTablename() {
        return dicomThumbnailTablename;
    }

    public String getDicomThumbnailCf() {
        return dicomThumbnailCf;
    }

    public String getDicomDisensitizationCf() {
        return dicomDisensitizationCf;
    }

    public String getIndexKfb() {
        return indexKfb;
    }

    public String getTypeKfb() {
        return typeKfb;
    }

    public String getKfbTablename() {
        return kfbTablename;
    }

    public String getKfbCf() {
        return kfbCf;
    }
    public String getDirPrefixKfb() {
        return dirPrefixKfb;
    }

    public String getEdfTablename() {
        return edfTablename;
    }

    public String getEdfCf() {
        return edfCf;
    }

    public String getIndexEdf() {
        return indexEdf;
    }

    public String getTypeEdf() {
        return typeEdf;
    }

    public String getDirPrefixGuagetable() {
        return dirPrefixGuagetable;
    }

    public String getIndexGuagetable() {
        return indexGuagetable;
    }

    public String getTypeGuagetable() {
        return typeGuagetable;
    }

    public String getGuagetableTablename() {
        return guagetableTablename;
    }

    public String getGuagetableCf() {
        return guagetableCf;
    }

    public String getTempdir() {
        return tempdir;
    }

    public String getDcm2jpgScriptPath() {
        return dcm2jpgScriptPath;
    }

    public String getDicomSeriesuidTablename() {
        return dicomSeriesuidTablename;
    }

    public String getDicomSeriesuidCf() {
        return dicomSeriesuidCf;
    }
}
