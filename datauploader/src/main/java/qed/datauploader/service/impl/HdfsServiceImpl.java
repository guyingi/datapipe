package qed.datauploader.service.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import qed.datauploader.config.UploaderConfiguration;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.service.HdfsService;
import qed.datauploader.sinker.Uploader;
import yasen.dicom.DicomWritable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class HdfsServiceImpl implements HdfsService {
    static Logger logger = Logger.getLogger(Uploader.class.getName());
    static UploaderConfiguration uploaderConf = new UploaderConfiguration();

    /**
     * 将一个序列下的所有dicom文件写入一个SequenceFile里面
     * @param seriesDir
     * @param finalPositionPath
     * @param conf
     * @return
     */
    @Override
    public int upCommonDicomToHdfs(File seriesDir, String finalPositionPath, Configuration conf) {
        Path desPath = new Path(uploaderConf.getDefaultFS()+finalPositionPath);

        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(desPath), SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(DicomWritable.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
        } catch (IOException e) {
            e.printStackTrace();
            return SysConsts.FAILED;
        }

        FileInputStream fin = null;
        byte []data = null;
        try {
            if(seriesDir.isDirectory()){
                /**如果是目录需要循环上传其下所有文件*/
                for(File e : seriesDir.listFiles()) {
                    fin = new FileInputStream(e);
                    int available = fin.available();
                    data = new byte[available];
                    int length = fin.read(data);
                    if(length!=available){
                        return SysConsts.FAILED;
                    }
                    Text key = new Text(e.getName());
                    DicomWritable value = new DicomWritable(data);
                    writer.append(key, value);
                }
            }else if(seriesDir.isFile()){
                /**如果是文件只需要上传该文件*/
                fin = new FileInputStream(seriesDir);
                int available = fin.available();
                data = new byte[available];
                int length = fin.read(data);
                if(length!=available){
                    return SysConsts.FAILED;
                }
                Text key = new Text(seriesDir.getName());
                DicomWritable value = new DicomWritable(data);
                writer.append(key, value);
            }
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return SysConsts.FAILED;
        } catch (IOException e1) {
            e1.printStackTrace();
            return SysConsts.FAILED;
        }finally {
            try {
                writer.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            IOUtils.closeStream(writer);
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public int upDicomDesensitization(String localDir, String remoteDir, Configuration hdfsconf) throws IOException {
        File localDirFile = new File(localDir);
        FileSystem fs = FileSystem.get(hdfsconf);
        for(File file : localDirFile.listFiles()){
            fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(remoteDir+SysConsts.LEFT_SLASH+file.getName()));
        }
        return SysConsts.SUCCESS;
    }

    /**
     * 这个是文件到文件
     * @param localFilePath
     * @param remoteFilePath
     * @param hdfsconf
     * @return
     */
    @Override
    public int upFile(String localFilePath, String remoteFilePath,Configuration hdfsconf) {
        FileSystem fs;
        try {
            fs = FileSystem.get(hdfsconf);
            fs.copyFromLocalFile(new Path(localFilePath),new Path(remoteFilePath));
        } catch (IOException e) {
            e.printStackTrace();
            return SysConsts.FAILED;
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public boolean del(String hdfspath,Configuration hdfsconf) throws IOException {
        if(StringUtils.isBlank(hdfspath)){
            return false;
        }
        FileSystem fs = null;
        try {
            fs = FileSystem.get(hdfsconf);
            Path path = new Path(hdfspath);
            fs.delete(path,true);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(fs != null)
                fs.close();
        }
        return false;
    }


}
