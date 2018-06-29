package qed.downloaddesensitization;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package qed.downloaddesensitization
 * @Description: ${todo}
 * @date 2018/6/26 10:22
 */
public class Tool {
    //n不能大于9，因为int类型位数限制
    public static String formatDigitalToNBit(String numberStr,int n){
        String result = "0000000000"+numberStr;
        result = result.substring(result.length()-n,result.length());
        return result;
    }

    //将原目录下面的子文件拷贝到目标目录下
    public static void copyDir(String srcDir,String desDir){
        for(File file : new File(srcDir).listFiles()){
            String name = file.getName();
            String desFilePath = desDir+File.separator+name;
            File desFile = new File(desFilePath);
            try {
                FileUtils.copyFile(file,desFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 清空文件夹，只是删除子文件，传入的目录不做删除
     */
    public static boolean delAllFile(String path) {
        boolean flag = false;
        File file = new File(path);
        if (!file.exists()) {
            return flag;
        }
        if (!file.isDirectory()) {
            return flag;
        }
        String[] tempList = file.list();
        File temp = null;
        for (int i = 0; i < tempList.length; i++) {
            if (path.endsWith(File.separator)) {
                temp = new File(path + tempList[i]);
            } else {
                temp = new File(path + File.separator + tempList[i]);
            }
            if (temp.isFile()) {
                temp.delete();
            }
            if (temp.isDirectory()) {
                delAllFile(path + "/" + tempList[i]);//先删除文件夹里面的文件
                delFolder(path + "/" + tempList[i]);//再删除空文件夹
                flag = true;
            }
        }
        return flag;
    }
    /*********删除整个文件夹，包括所有子文件和本文件夹************/
    public static void delFolder(String folderPath) {
        try {
            delAllFile(folderPath); //删除完里面所有内容
            String filePath = folderPath;
            filePath = filePath.toString();
            File myFilePath = new File(filePath);
            myFilePath.delete(); //删除空文件夹
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void copyDirToLocal(String hdfspath, String local, Configuration hdfsconf) {
        String name = hdfspath.substring(hdfspath.lastIndexOf("/")+1,hdfspath.length());
        String localDir = local + File.separator+name;
        Path remotePath = new Path(hdfspath);
        Path lcoalPath = new Path(localDir);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(hdfsconf);
            fs.copyToLocalFile(remotePath,lcoalPath);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getRunnerPath(){
        String rootPath = "";
        String path = Tool.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if("windows".equals(getOS())){
            rootPath = path.substring(1, path.length()-1);
            rootPath = rootPath.replace("/","\\");
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
