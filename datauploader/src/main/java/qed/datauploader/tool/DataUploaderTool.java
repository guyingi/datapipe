package qed.datauploader.tool;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import qed.datauploader.consts.SysConsts;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

public class DataUploaderTool {
    static Pattern pattern = Pattern.compile("^([\\d|a-zA-Z]{4},[\\d|a-zA-Z]{4})$");
    static Random random = new Random(System.currentTimeMillis());
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    static Logger logger = Logger.getLogger(DataUploaderTool.class);

    public static void main(String[] args) {

        try {
            Date parse = sdf.parse("20100204");
            System.out.println(parse.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    synchronized public static String formatDate(String date){
        if(date!=null && date.length()!=0){
            return date.substring(0,4)+SysConsts.LINE+date.substring(4,6)+SysConsts.LINE+date.substring(6,8);
        }else{
            return null;
        }
    }

    synchronized public static int formatInteger(String data){
        if(data!=null && data.length()!=0){
            try{
                return Integer.parseInt(data);
            }catch (NumberFormatException e){
                return 0;
            }
        }
        return 0;
    }

    synchronized public static long formatLong(String data){
        if(data!=null && data.length()!=0){
            try{
                return Long.parseLong(data);
            }catch (NumberFormatException e){
                return 0l;
            }
        }
        return 0l;
    }

    synchronized public static double formatDouble(String data){
        if(data!=null && data.length()!=0){
            try{
                return Double.parseDouble(data);
            }catch (NumberFormatException e){
                return 0d;
            }
        }
        return 0d;
    }

    synchronized public static void recordLog(Logger logger, String seriesDir, boolean isSuccess, String msg){
        if(isSuccess){
            logger.log(Level.INFO,seriesDir+":"+msg);
        }else{
            logger.log(Level.ERROR,"失败序列目录："+seriesDir+";失败原因："+msg);
        }
    }

    /**
     *  传入总目录，返回所有序列的目录
     * @param dirPath
     * @return
     */
    public static List<String> listSeriesDir(String dirPath){
        List<String> fileList = new LinkedList<>();
        File file = new File(dirPath);
        listFilehelp(file,fileList);
        return fileList;
    }
    private static void listFilehelp(File file,List<String> fileList){
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for(File e : files) {
                if(e.isFile()&&isDicom(e)){
                    fileList.add(e.getParentFile().getAbsolutePath());
                    break;
                }else {
                    listFilehelp(e,fileList);
                }
            }
        }else if(file.isFile()&&isDicom(file)){
            fileList.add(file.getParentFile().getAbsolutePath());
        }
    }

    /**
     * 扫描目录，列出后缀为参数suffix的所有文件绝对路径
     * @param path
     * @return
     */
    public static List<String> listEdfPath(String path,String suffix){
        List<String> list = new ArrayList<String>();
        File file = new File(path);
        listEdfPathHelp(file,list,suffix);
        return list;
    }

    private static void listEdfPathHelp(File file,List<String> list,String suffix){
        if(file.isFile() && file.getName().endsWith(suffix)){
            list.add(file.getAbsolutePath());
        }else if(file.isDirectory()){
            File[] files = file.listFiles();
            for(File e : files){
                if(e.isFile() && e.getName().endsWith(suffix)){
                    list.add(e.getAbsolutePath());
                }else if(e.isDirectory()){
                    listEdfPathHelp(e,list,suffix);
                }
            }
        }
    }

    /**
     * 取上线，几个月算1岁，1岁零几个月算2岁，依次类推
     * @param studyDate
     * @param birthDay
     * @return
     */
    public static int getPatientAge(String studyDate,String birthDay){
        Calendar cal = Calendar.getInstance();
        if(StringUtils.isBlank(studyDate) || StringUtils.isBlank(birthDay)){
            return 0;
        }
        try {
            Date studydate = sdf.parse(studyDate);
            Date birthdate = sdf.parse(birthDay);

            cal.setTime(studydate);

            int studyyear = cal.get(Calendar.YEAR);
            int studymonth = cal.get(Calendar.MONTH);
            int studyday = cal.get(Calendar.DAY_OF_MONTH);
            cal.setTime(birthdate);
            int birthyear = cal.get(Calendar.YEAR);
            int birthmonth = cal.get(Calendar.MONTH);
            int birthday = cal.get(Calendar.DAY_OF_MONTH);
            int age = studymonth>birthmonth? studyyear-birthyear+1:studyyear-birthyear;
            return age;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 返回特定日期字符串，20180529
     * @param fullFileName
     * @return
     */
    public static String getLastModifyDateOfFile(String fullFileName){
        System.out.println(fullFileName);
        Path path=Paths.get(fullFileName);
        BasicFileAttributeView basicview=Files.getFileAttributeView(path, BasicFileAttributeView.class,LinkOption.NOFOLLOW_LINKS );
        BasicFileAttributes attr;
        Calendar cal = Calendar.getInstance();
        try {
            attr = basicview.readAttributes();
            Date createDate = new Date(attr.lastModifiedTime().toMillis());
            cal.setTime(createDate);
            String year = cal.get(Calendar.YEAR)+"";
            String month = formatDigitalToNBit(cal.get(Calendar.MONTH)+"",2);
            String day = cal.get(Calendar.DAY_OF_MONTH)+"";
            return year+month+day;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "19700101";
    }



    /**
     * 判断是不是dicom文件，如果是则返回true,否则返回false
     * @param file
     * @return
     */
    synchronized private static boolean isDicom(File file){
        return !IMAParseTool.isBadDicom(file.getAbsolutePath());
    }

    public static String getMD5(String source){
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(source.getBytes("UTF-8"));
            byte[] md5Array = md5.digest();
            return bytesToHexString(md5Array);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String bytesToHexString(byte[] src){
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    /**
     * 生成n位随机数
     * @return
     */
    public static int generateRandonNumber(int n){
        int bound = 1;
        while(n-->1)
            bound*=10;
        int temp = 0;
        while(bound>(temp=random.nextInt(bound*10))){}
        return temp;
    }

    //CRC32,获取字符串source n位循环冗余数
    public static String getCRC32(String source,int n){
        long width = 1;
        while(n-->0) width*=10;
        CRC32 crc32 = new CRC32();
        long result = 0;
        try {
            crc32.update(source.getBytes("UTF-8"));
            result = crc32.getValue()%width;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result+"";
    }

    public static long getTimeStamp(){
        return new Date().getTime();
    }

    public static String getTodayDate(){
        return sdf.format(new Date());
    }

    /**
     * 格式化字符串日期20170302 to /2017/03/02
     * 如果解析出错，时间默认为19270/01/01
     * @param dateString
     * @return
     */
    public static String parseDateToPath(String dateString){
        String year = "1970";
        String month = "01";
        String day = "01";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            if(dateString != null) {
                date = sdf.parse(dateString);
            }
        } catch (ParseException e) {
            logger.log(Level.ERROR,e);
            e.printStackTrace();
        }
        if(date != null){
            Calendar instance = Calendar.getInstance();
            instance.setTime(date);
            year = instance.get(Calendar.YEAR)+"";
            month = instance.get(Calendar.MONTH)+1+"";
            day = instance.get(Calendar.DAY_OF_MONTH)+"";
        }
        return SysConsts.LEFT_SLASH+year+ SysConsts.LEFT_SLASH+month+ SysConsts.LEFT_SLASH+day;
    }

    //n不能大于9，因为int类型位数限制
    public static String formatDigitalToNBit(String numberStr,int n){
        String result = "0000000000"+numberStr;
        result = result.substring(result.length()-n,result.length());
        return result;
    }

    public static String formatRowkeyToNBit(String rowkey,int n){
        //100位
        String str = rowkey+"0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
        char[] chars = str.toCharArray();
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<n; i++){
            sb.append(chars[i]);
        }
        return sb.toString();
    }

    public static String getDateOfToday(){
        return sdf.format(new Date());
    }


    public static boolean isTag(String param){
        Matcher matcher = pattern.matcher(param);
        if(matcher.matches()){
            return true;
        }
        return false;
    }

    public static String getRunnerPath(){
        String rootPath = "";
        String path = DataUploaderTool.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if("windows".equals(getOS())){
            rootPath = path.substring(1, path.length()-1);
            rootPath = rootPath.replace(SysConsts.LEFT_SLASH,"\\");
        }else if("linux".equals(getOS())){
            //file:/home/ms/project/microservice/infosupplyer-1.0-SNAPSHOT.jar!/BOOT-INF/classes!/
            String tempArr[] = path.split("/");
            for(String e : tempArr){
                if(!e.endsWith(".jar")){
                    if(e.length()!=0)
                        rootPath += "/"+e;
                }else{
                    break;
                }
            }
        }
        return rootPath;
    }

    public static String getOS(){
        Properties prop = System.getProperties();
        String os = prop.getProperty("os.name");
        if(os.startsWith("win")|| os.startsWith("Win")){
            return "windows";
        }else if(os.startsWith("Linux")|| os.startsWith("linux")){
            return "linux";
        }else{
            return "";
        }
    }
}
