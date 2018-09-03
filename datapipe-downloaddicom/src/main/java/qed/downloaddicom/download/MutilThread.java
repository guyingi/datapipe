package qed.downloaddicom.download;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import yasen.dicom.DicomWritable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class MutilThread implements Runnable {
    static Logger logger = Logger.getLogger(Downloader.class);
    List<String> paths;
    String des;

    public MutilThread(){}

    public MutilThread(List<String> paths,String des){
        this.paths = paths;
        this.des = des;
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        System.out.println(name+" is start.. 任务："+paths.size());
        download(paths,des);
    }

    public boolean download(List<String> paths, String localPath){
        if(paths==null || paths.size()==0)
            return false;
        boolean isSuccess = false;
        if(localPath.endsWith("\\")||localPath.endsWith("//")){
            localPath = localPath.substring(0,localPath.length()-1);
        }
        for(String e : paths){
            if(downloadSingleSeries(e,localPath)){
                isSuccess = true;
                System.out.println("下载："+e);
            }else{
                System.out.println("失败："+e);
            }
        }
        return isSuccess;
    }

    private boolean downloadSingleSeries(String hdfsPath,String localPath){
        boolean isSuccess = true;
        String dirname = hdfsPath.substring(hdfsPath.lastIndexOf("/")+1,hdfsPath.length());
//        System.out.println(localPath+dirname);
        File localSeriaDir = new File(localPath+File.separator +dirname);
        if(!localSeriaDir.exists()) {
            localSeriaDir.mkdirs();
        }
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        String hdfspre = conf.get("fs.defaultFS");
        Path path = new Path(hdfspre+hdfsPath);
        SequenceFile.Reader.Option option = SequenceFile.Reader.file(path);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf,option);
            Text key = (Text) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            DicomWritable value = (DicomWritable)ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            if(value==null){
            }
            FileOutputStream fout = null;
            int count = 0;
            while (reader.next(key, value)) {

                String filepath = localPath+File.separator+dirname+File.separator+key.toString();
                File file = new File(filepath);
                if(!file.exists())
                    file.createNewFile();
                fout = new FileOutputStream(file);
                logger.log(Level.DEBUG,"本地dicom路径："+filepath);

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
    }

    public static void main(String[] args) {
        String hdfspath = "/yasen/bigdata/dicom/2016/12/9/1x2x840x113681x2229455454x932x3683232165x676x1";
        String local = "F:\\实验室\\download";
        new MutilThread().downloadSingleSeries(hdfspath,local);
    }
}
