package yasen.toground.download;

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
import java.util.List;

public class MutilThread implements Runnable {
    static Logger logger = Logger.getLogger(Downloader.class);
    List<String> paths;
    String des;

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
        String delimiter =  getDelimiter();
        if(!(localPath.endsWith("\\")||localPath.endsWith("//"))){
            localPath = localPath+delimiter;
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
        File localSeriaDir = new File(localPath+dirname);
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

                String filepath = localPath+getDelimiter()+dirname+getDelimiter()+key.toString();
                File file = new File(filepath);
                if(!file.exists())
                    file.createNewFile();
                fout = new FileOutputStream(file);
                logger.log(Level.INFO,"本地dicom路径："+filepath);

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

    private String getDelimiter(){
        String delimiter = "/";
        String osType = System.getProperty("os.name");
        if(osType.startsWith("Windows")){
            delimiter = "\\";
        }
        return delimiter;
    }
}
