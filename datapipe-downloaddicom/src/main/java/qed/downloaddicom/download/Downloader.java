package qed.downloaddicom.download;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * 参数接收json文件位置，
 * 参数接收下载文件存放目录
 * 日志就放在当前运行目录
 */
public class Downloader {
    static Logger logger = Logger.getLogger(Downloader.class);

  /*  public boolean download(List<String> paths, String localPath){
        if(paths==null || paths.size()==0)
            return false;
        boolean isSuccess = false;
        String delimiter =  getDelimiter();
        if(!(localPath.endsWith("\\")||localPath.endsWith("//"))){
            localPath = localPath+delimiter;
        }
        for(String e : paths){
            //有一个下载成功就判定为成功
            System.out.println("路径："+e);
            if(downloadSingleSeries(e,localPath)){
                isSuccess = true;
            }
        }
        return isSuccess;
    }*/

    /**
     * @Author:weiguangwu
     * @Description:下载单个序列到本地目录
     * @params:[hdfsPath, localPath]
     * @return: boolean
     * @Date: 2018/4/23 17:24
     */
   /* private boolean downloadSingleSeries(String hdfsPath,String localPath){
        boolean isSuccess = true;
        String dirname = hdfsPath.substring(hdfsPath.lastIndexOf("/")+1,hdfsPath.length());
        System.out.println(localPath+dirname);
        File localSeriaDir = new File(localPath+dirname);
        if(!localSeriaDir.exists()) {
            localSeriaDir.mkdirs();
        }
        Configuration conf = new Configuration();
        Path path = new Path(hdfsPath);
        SequenceFile.Reader.Option option = SequenceFile.Reader.file(path);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf,option);
            Text key = (Text) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            uploader.DicomWritable value = (uploader.DicomWritable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            FileOutputStream fout = null;
            int count = 0;
            while (reader.next(key, value)) {

                String filepath = localPath+getDelimiter()+dirname+getDelimiter()+key.toString();
                File file = new File(filepath);
                if(!file.exists())
                    file.createNewFile();
                fout = new FileOutputStream(file);
                logger.log(Level.INFO,"本地dicom路径："+filepath);
                System.out.println("本地路径dicom："+filepath);

                byte[] data = value.getData();
                fout.write(data);
                fout.flush();
                fout.close();
                count++;
            }
            logger.log(Level.INFO,"下载文件数："+count);
//            System.out.println("下载文件数："+count);
        } catch (IOException e) {
            isSuccess = false;
            logger.log(Level.INFO,"下载失败文件："+hdfsPath);
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
        return isSuccess;
    }*/

    public static List<String> parseJSON(String file){
        List<String> list = new LinkedList<String>();
        String text = null;
        try {
            InputStream inputStream = new FileInputStream(file);
            text = org.apache.commons.io.IOUtils.toString(inputStream,"utf8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(text==null){
            return null;
        }
        JSONObject json = new JSONObject();
        JSONReader reader = new JSONReader(new StringReader(text));
        reader.startObject();
        while (reader.hasNext()) {
            String key = reader.readString();
            if (key.equals("data")) {
                reader.startArray();
                while (reader.hasNext()) {
                    reader.startObject();
                    while (reader.hasNext()) {
                        String k = reader.readString();
                        if("hdfspath".equals(k)){
                            String v = reader.readString();
                            list.add(v);
                        }else {
                            String v = reader.readString();
                        }
                    }
                    reader.endObject();
                }
                reader.endArray();
            }else if(key.equals("code")) {
                reader.readString();
            }else {
                reader.readInteger();
            }
        }
        reader.endObject();
        return list;
    }


    private String getDelimiter(){
        String delimiter = "/";
        String osType = System.getProperty("os.name");
        if(osType.startsWith("Windows")){
            delimiter = "\\";
        }
        return delimiter;
    }
}
