package qed.datauploader.tool;

import com.alibaba.fastjson.JSONObject;
import ij.plugin.DICOM;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.config.UploaderConfiguration;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IMAParseTool {
    volatile static DICOM dicom;
    volatile static SimpleDateFormat sdf;
    volatile static Pattern pattern;
    volatile static UploaderConfiguration uploaderConfiguration;

    static{
        dicom = new DICOM();
        sdf = new SimpleDateFormat("yyyyMMdd");
        pattern = Pattern.compile("^([\\d|a-zA-Z]{4},[\\d|a-zA-Z]{4})[\\s]{2}(.*)$");
        uploaderConfiguration = new UploaderConfiguration();
    }

    public static void main(String[] args) {
        long start = new Date().getTime();
//        String str = "F:\\dicom\\正常对照\\30-40\\nan\\li haiyan,38\\LI_HAIYAN.PT.PET_0_PUMC_PETCT_BRAIN_(ADULT).0003.0117.2015.12.07.13.44.45.687500.216639238.IMA";
//        String str = "F:\\实验室\\li haiyan,38\\LI_HAIYAN.PT.PET_0_PUMC_PETCT_BRAIN_(ADULT).0003.0117.2015.12.07.13.44.45.687500.216639238.IMA";
//          String str = "F:\\dicom\\Dicom\\LI_SHUQIANG.NM.BRAIN_SCAN.1000.0001.2016.08.18.14.53.08.468750.2034113.IMA";
//          String str = "F:\\dicom\\PA025\\ST0\\SE0";
//        printInfo("F:\\实验室\\li haiyan,38\\LI_HAIYAN.PT.PET_0_PUMC_PETCT_BRAIN_(ADULT).0003.0001.2015.12.07.13.44.45.687500.216631640.IMA");
//        String str = "F:\\实验室\\download\\44105556736592\\PANG GUI RONG-44105556736592527707";
//        JSONObject jsonObject = extractJsonMsg("F:\\dicom\\PA025\\ST0\\SE13\\IM0");
//        System.out.println(jsonObject.toString());
//        String path = "F:\\dicom\\new\\des\\00000c6d-dc77763e-5249ac89-f0aeae65-027a8812\\EY624867 Dong Guo Qin\\ZX1845538 ScreeningBilateral Mammography\\MG R CC\\MG000000.dcm";
        String path = "F:\\实验室\\EY795397 Cai Huan Ru\\ZX1908149 chest\\CT-2\\CT000001.dcm";
        printInfo(path);
//        String dirPath  ="F:\\dicom\\dicom\\PETMR\\1\\PET";
//        DICOM dicom = new DICOM();
//        Map<String,String> map =  new HashMap<String,String>();
//        List<String> seriesDirList = DataUploaderTool.listSeriesDir(dirPath);
//        int n = 0;
//        for(String dir : seriesDirList){
//            File file = new File(dir);
//            for(File f : file.listFiles()){
//                if(!IMAParseTool.isBadDicom(f.getAbsolutePath())){
//                    n++;
//                    String info = dicom.getInfo(f.getAbsolutePath());
//                    String[] arr = info.split("\n");
//                    System.out.println(arr.length);
//                    for(String e : arr){
//                        if((!e.contains("---"))&&e.startsWith("0")){
//                            try{
//                                if(!map.containsKey(e.substring(0,9))){
//                                    map.put(e.substring(0,9),e);
//                                }
//                            }catch (StringIndexOutOfBoundsException a){
//                                System.out.println("错误"+e);
//                            }
//                        }
//                    }
//                    break;
//                }
//            }
//        }
//        for(Map.Entry<String,String> entry: map.entrySet()){
//            System.out.println(entry.getValue());
//        }
//        System.out.println("总数："+n);
//        String str = "C:\\Users\\WeiGuangWu\\IdeaProjects" +
//                "\\bigdata\\infosupplyer\\target\\classes\\dicomtemp\\1284011370411119432150840119112\\CT000002.dcm";
//
//        printInfo(str);
        long end = new Date().getTime();
    }


    /**
     *
     * @param dicomFilePath
     * @return
     */
    public static JSONObject extractMetaMsg(String dicomFilePath){
        JSONObject json = new JSONObject();
        String info;
        try {
            info = dicom.getInfo(dicomFilePath);
            if(info.length()==0)
                return json;
        }catch(Exception e){
            return json;
        }

        String []temparr = info.split("[\r\n]");
        for(String s : temparr){
            Matcher matcher = pattern.matcher(s);
            if(matcher.matches()){
                String temp = matcher.group(1).trim();
                if(SysConsts.DICOM_META_TAGS.contains(temp)){
                    json.put(temp,matcher.group(2).split(":")[1].trim());
                }
            }
        }
        return json;
    }

    public static JSONObject extractExactField(String dicomFilePath,String key){
        JSONObject json = new JSONObject();
        String info = "";
        if(isBadDicom(dicomFilePath)){
            return json;
        }
        info = dicom.getInfo(dicomFilePath);

        String []temparr = info.split("[\r\n]");
        for(String s : temparr){
            Matcher matcher = pattern.matcher(s);
            if(matcher.matches()){
                String temp = matcher.group(1).trim();
                if(key.equals(temp)){
                    json.put(temp,matcher.group(2).split(":")[1].trim());
                    break;
                }
            }
        }
        return json;
    }

    /****************这个方法暂时不使用，功能是将所有元数据字段初始化********************/
//    private static void initMetaJson(JSONObject metaJson){
//        for(Map.Entry<String,String> entry : SysConsts.DCM_META_TAG2KW.entrySet()){
//            String value = entry.getValue();
//            if(SysConsts.ES_INTEGER_TYPE_FIELD.contains(value))
//                metaJson.put(value,"0");
//            if(SysConsts.ES_DOUBLE_TYPE_FIELD.contains(value))
//                metaJson.put(value,"0.0");
//            metaJson.put(value,"");
//        }
//    }


    public static void printInfo(String filePath) {
        try {
            DICOM dicom = new DICOM();
            String s = dicom.getInfo(filePath);
//            dicom.run(filePath);
            System.out.println(s);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("错误" );
        }
    }

    public static boolean isBadDicom(String dicomPath){
       try{
           String str = dicom.getInfo(dicomPath);
           if(str.length()==0){
               return true;
           }
       }catch (Exception e){
           return true;
       }
       return false;
    }


}

//    String s = "0008,0020  Study Date: 20170110";
//    // 把要匹配的字符串写成正则表达式，然后要提取的字符使用括号括起来
//    // 在这里，我们要提取最后一个数字，正则规则就是“一个数字加上大于等于0个非数字再加上结束符”
////        Pattern pattern = Pattern.compile("(^[\\d]{4},[\\d|a-zA-Z]{4})[\\s]{2}[a-zA-Z|'|-|\\s]*:([\\s\\S]*)$");
//    Pattern pattern = Pattern.compile("^([\\d]{4},[\\d|a-zA-Z]{4})[\\s]{2}(.*)$");
//    Matcher matcher = pattern.matcher(s);
//        if(matcher.matches())
//                System.out.println(matcher.group(1)+" : "+matcher.group(2).split(":")[1]);