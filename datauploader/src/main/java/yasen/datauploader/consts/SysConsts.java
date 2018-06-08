package yasen.datauploader.consts;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import yasen.datauploader.config.UploaderConfiguration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class SysConsts {

    /**********dicom 元数据tag-keyword对照表*******************/
    public static List<String> DICOM_META_TAGS = new ArrayList<String>();
    public static Map<String,String> DCM_META_TAG2KW = new HashMap<String,String>();
    public static Map<String,String> DCM_META_KW2TAG = new HashMap<String,String>();

    /********************下面这个是dicom元数据标准**************************************/
    static{
        InputStreamReader reade2 = new InputStreamReader(SysConsts.class.getClassLoader().getResourceAsStream("DicomMetaElements.properties"));
        Properties props2 = new Properties();
        try {
            props2.load(reade2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Enumeration<?> enumeration = props2.propertyNames();
        while(enumeration.hasMoreElements()){
            String name = (String)enumeration.nextElement();
            DICOM_META_TAGS.add(name);
            DCM_META_TAG2KW.put(name,props2.getProperty(name));
            DCM_META_KW2TAG.put(props2.getProperty(name),name);
        }
    }


    public static String DOT = ".";
    public static String LEFT_SLASH= "/";
    public static String RIGHT_SLASH= "\\";
    public static String LINE= "-";
    public static String SPACE = " ";
    public static Integer THREAD_NUMBER = 1;
    public static Integer SUCCESS = 0;
    public static Integer EXISTS = 1;
    public static Integer FAILED = -1;


    /**********临时目录****************************/
    public static String SCRIPT_DIRNAME = "script";
    public static String TEMP_DIRNAME = "temp";



    /************上载器是没有区间的****************/
    //范围值的字段：年龄段age-section，检查日期段studydate，数据收入日期段entrydatesection
//    public static String AGE_SECTION = "age_section";
//    public static String STUDYDATE_SECTION = "studydate_section";
//    public static String ENTRYDATE_SECTION = "entrydate_section";

    /*********************编码描述映射*************************/
/*    public static String MediaStorageSOPClassUID_CODE	  = "0002,0002";
    public static String MediaStorageSOPInstUID_CODE      = "0002,0003";
    public static String TransferSyntaxUID_CODE            = "0002,0010";
    public static String ImplementationClassUID_CODE       = "0002,0012";
    public static String ImplementationVersionName_CODE	  = "0002,0013";
    public static String SourceApplicationEntityTitle_CODE = "0002,0016";
    public static String SpecificCharacterSet_CODE         = "0008,0005";
    public static String ImageType_CODE                    = "0008,0008";
    public static String InstanceCreationDate_CODE         = "0008,0012";
    public static String InstanceCreationTime_CODE         = "0008,0013";
    public static String InstanceCreatorUID_CODE           = "0008,0014";
    public static String SOPClassUID_CODE                  = "0008,0016";
//    public static String SOPInstanceUID_CODE               = "0008,0018"; //因为以序列为单位存储，所以不存储单个dicom序号
    public static String StudyDate_CODE                    = "0008,0020";
    public static String SeriesDate_CODE                   = "0008,0021";
    public static String AcquisitionDate_CODE              = "0008,0022";
    public static String ImageDate_CODE                    = "0008,0023";
    public static String StudyTime_CODE                    = "0008,0030";
    public static String SeriesTime_CODE                   = "0008,0031";
    public static String AcquisitionTime_CODE              = "0008,0032";
    public static String ImageTime_CODE                    = "0008,0033";
    public static String AccessionNumber_CODE              = "0008,0050";
    public static String Modality_CODE                     = "0008,0060";
    public static String Manufacturer_CODE                 = "0008,0070";
    public static String InstitutionName_CODE              = "0008,0080";
    public static String ReferringPhysiciansName_CODE     = "0008,0090";
    public static String TimezoneOffsetFromUTC_CODE        = "0008,0201";
    public static String StationName_CODE                  = "0008,1010";
    public static String StudyDescription_CODE             = "0008,1030";
    public static String SeriesDescription_CODE            = "0008,103E";  //序列描述
    public static String ManufacturersModelName_CODE       = "0008,1090";   //医院
    public static String ReferencedSOPClassUID_CODE        = "0008,1150";
    public static String ReferencedSOPInstanceUID_CODE     = "0008,1155";
    public static String PatientName_CODE                  = "0010,0010";
    public static String PatientID_CODE                    = "0010,0020";
    public static String PatientsBirthDate_CODE            = "0010,0030";
    public static String PatientsSex_CODE                  = "0010,0040";
    public static String PatientsAge_CODE                  = "0010,1010";
    public static String PatientsSize_CODE                 = "0010,1020";
    public static String PatientsWeight_CODE               = "0010,1030";
    public static String SliceThickness_CODE               = "0018,0050";
    public static String SoftwareVersions_CODE             = "0018,1020";
    public static String ReconstructionDiameter_CODE       = "0018,1100";
    public static String GantryDetectorTilt_CODE           = "0018,1120";
    public static String FieldOfViewShape_CODE             = "0018,1147";
    public static String FieldOfViewDimensions_CODE        = "0018,1149";
    public static String CollimatorType_CODE               = "0018,1181";
    public static String ConvolutionKernel_CODE            = "0018,1210";
    public static String ActualFrameDuration_CODE          = "0018,1242";
    public static String PatientPosition_CODE              = "0018,5100";
    public static String StudyInstanceUID_CODE             = "0020,000D";
    public static String SeriesInstanceUID_CODE            = "0020,000E";
    public static String StudyID_CODE                      = "0020,0010";
    public static String SeriesNumber_CODE                 = "0020,0011";
    public static String ImageNumber_CODE                  = "0020,0013"; //PET文件有该字段，一个序列为单位，不需要图片序号
    public static String ImagePositionPatient_CODE         = "0020,0032";
    public static String ImageOrientationPatient_CODE      = "0020,0037";
    public static String FrameOfReferenceUID_CODE         = "0020,0052";
    public static String PositionReferenceIndicator_CODE   = "0020,1040";
    public static String SliceLocation_CODE                = "0020,1041";
//    public static String NumberOfSlices_CODE               = "0054,0081";  //有的dicom文件没有该字段，弃用
    public static String ImageNumber = "ImageNumber";//图片在该序列中序号

    public static String NumberOfFrames_CODE                = "0028,0008";   //特殊dicom文件才有的,Number of Frames*/

    /********************dicom在ES中存储的字段**************************/
//    public static String MediaStorageSOPClassUID	  = "MediaStorageSOPClassUID";
//    public static String MediaStorageSOPInstUID       = "MediaStorageSOPInstUID";
//    public static String TransferSyntaxUID            = "TransferSyntaxUID";
//    public static String ImplementationClassUID       = "ImplementationClassUID";
//    public static String ImplementationVersionName	  = "ImplementationVersionName";
//    public static String SourceApplicationEntityTitle = "SourceApplicationEntityTit";
//    public static String SpecificCharacterSet         = "SpecificCharacterSet";
//    public static String ImageType                    = "ImageType";
//    public static String InstanceCreationDate         = "InstanceCreationDate";
//    public static String InstanceCreationTime         = "InstanceCreationTime";
//    public static String InstanceCreatorUID           = "InstanceCreatorUID";
//    public static String SOPClassUID                  = "SOPClassUID";
////    public static String SOPInstanceUID               = "SOPInstanceUID"; //一个序列为单位，不需要单张图片标识号
//    public static String AcquisitionDate              = "AcquisitionDate";
//    public static String ImageDate                    = "ImageDate";
//    public static String AccessionNumber              = "AccessionNumber";
//    public static String Modality                     = "Modality";
//    public static String Manufacturer                 = "Manufacturer";
//    public static String InstitutionName              = "InstitutionName";
//    public static String ReferringPhysiciansName     = "ReferringPhysicians Name";
//    public static String TimezoneOffsetFromUTC        = "TimezoneOffsetFromUTC";
//    public static String StationName                  = "StationName";
//    public static String StudyDescription             = "StudyDescription";
//    public static String SeriesDescription            = "SeriesDescription";
//    public static String ManufacturersModelName       = "ManufacturersModelName";
//    public static String ReferencedSOPClassUID        = "ReferencedSOPClassUID";
//    public static String ReferencedSOPInstanceUID     = "ReferencedSOPInstanceUID";
//    public static String SoftwareVersions             = "SoftwareVersions";
//    public static String GantryDetectorTilt           = "GantryDetectorTilt";
//    public static String FieldOfViewShape             = "FieldOfViewShape";
//    public static String FieldOfViewDimensions        = "FieldOfViewDimensions";
//    public static String CollimatorType               = "CollimatorType";
//    public static String ConvolutionKernel            = "ConvolutionKernel";
//    public static String ActualFrameDuration          = "ActualFrameDuration";
//    public static String PatientPosition              = "PatientPosition";
//    public static String StudyInstanceUID             = "StudyInstanceUID";
//    public static String SeriesInstanceUID            = "SeriesInstanceUID";
//    public static String StudyID                      = "StudyID";
//    public static String ImagePositionPatient         = "ImagePositionPatient";
//    public static String ImageOrientationPatient      = "ImageOrientationPatient";
//    public static String FrameOfReferenceUID         = "Frameof ReferenceUID";
//    public static String PositionReferenceIndicator   = "PositionReferenceIndicator";


    /**************元数据表中自定义字段************************/
    public static String SeriesUID = "SeriesUID";//一个dicom文件的id，使用seriesuid+做CRC(用户名年龄性别)+图片序号，生成唯一id
    public static String PatientUID = "PatientUID"; //医院8位PatientID+姓名拼音+出生日期+性别(F/M)最后求MD5,需要在多医院中唯一
    public static String HDFSPATH = "hdfspath";//hdfs文件路径
    public static String ROWKEY =  "rowkey"; //dicom序列的hbase元数据表rowkey
    public static String ENTRYDATE = "entrydate";//录入日期
    //    public static String ORGAN = "organ";//器官

    /**************元数据表中代码中需要使用的字段************************/
    public static String PatientBirthDate_TAG		= "0010,0030";
    public static String SeriesDate_TAG            	= "0008,0021";
    public static String PatientID_TAG             	= "0010,0020";
    public static String PatientName_TAG           	= "0010,0010";
    public static String PatientAge_TAG            	= "0010,1010";
    public static String PatientSex_TAG            	= "0010,0040";
    public static String StudyDate_TAG             	= "0008,0020";
    public static String StudyID_TAG               	= "0020,0010";
    public static String SeriesInstanceUID_TAG     	= "0020,000E";
    public static String SliceThickness_TAG        	= "0018,0050";
    public static String ReconstructionDiameter_TAG	= "0018,1100";
    public static String SliceLocation_TAG         	= "0020,1041";
    public static String PatientSize_TAG          	= "0010,1020";
    public static String PatientWeight_TAG         	= "0010,1030";
    public static String ContentTime_TAG           	= "0008,0033";
    public static String AcquisitionTime_TAG       	= "0008,0032";
    public static String SeriesTime_TAG            	= "0008,0031";
    public static String StudyTime_TAG             	= "0008,0030";
    public static String InstanceNumber_TAG         = "0020,0013";  //图片在该序列中序号
    public static String SeriesNumber_TAG          	= "0020,0011";
    public static String NumberOfSlices_TAG        	= "0054,0081";  //病人此序列图片总量
    public static String NumberOfFrames_TAG         = "0028,0008";  //特殊dicom文件才有的,图片ImageNumber矩阵
    public static String InstitutionName_TAG        = "0008,0080";  //特殊dicom文件才有的,图片ImageNumber矩阵



    /**************edf电信号文件在es中元数据字段*************/


    /**************其他常量字段****************************/
    public static String WINDOWS = "windows";
    public static String LINUX = "linux";

    /**************HBase表列簇字段****************************/
    public static String THUMBNAIL = "thumbnail";
    public static String TAG = "tag"; //脱敏数据的表中使用该字段
    public static String CREATE_DATE = "createdate"; //kfb元数据字段中有使用
    public static String BARCODE = "barcode"; //kfb元数据字段中有使用

    /**************ES中为Integer类型的字段******************************/
    public static List<String> ES_INTEGER_TYPE_FIELD = new ArrayList<String>();
    static {
        ES_INTEGER_TYPE_FIELD.add(NumberOfSlices_TAG);
        ES_INTEGER_TYPE_FIELD.add(SeriesNumber_TAG);
        ES_INTEGER_TYPE_FIELD.add(PatientAge_TAG);
    }
    public static List<String> ES_LONG_TYPE_FIELD = new ArrayList<String>();
    static{
        ES_LONG_TYPE_FIELD.add(SysConsts.StudyTime_TAG);
        ES_LONG_TYPE_FIELD.add(SysConsts.SeriesTime_TAG);
        ES_LONG_TYPE_FIELD.add(SysConsts.AcquisitionTime_TAG);
        ES_LONG_TYPE_FIELD.add(SysConsts.ContentTime_TAG);
    }

    public static List<String> ES_DOUBLE_TYPE_FIELD = new ArrayList<String>();
    static {
        ES_DOUBLE_TYPE_FIELD.add(SliceLocation_TAG);
        ES_DOUBLE_TYPE_FIELD.add(PatientSize_TAG);
        ES_DOUBLE_TYPE_FIELD.add(PatientWeight_TAG);
        ES_DOUBLE_TYPE_FIELD.add(ReconstructionDiameter_TAG);
        ES_DOUBLE_TYPE_FIELD.add(SliceThickness_TAG);
    }

}
