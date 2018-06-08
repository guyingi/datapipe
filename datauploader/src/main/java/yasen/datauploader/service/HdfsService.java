package yasen.datauploader.service;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;

public interface HdfsService {

    /**
     * s上传普通dicom文件到hdfs
     * @param seriesDir
     * @param finalPositionPath
     * @param conf
     * @return
     */
    int upCommonDicomToHdfs(File seriesDir, String finalPositionPath, Configuration conf);

    /**
     * 上传脱敏数据到hdfs
     * @param localDir
     * @param remoteDir
     * @param hdfsconf
     * @return
     * @throws IOException
     */
    int upDicomDesensitization(String localDir,String remoteDir,Configuration hdfsconf) throws IOException;

    /**
     * 上传文件，简单复制
     * @param localFilePath
     * @param remoteFilePath
     * @param hdfsconf
     * @return
     * @throws IOException
     */
    int upFile(String localFilePath,String remoteFilePath,Configuration hdfsconf) throws IOException;


    boolean del(String hdfspath,Configuration hdfsconf) throws IOException;
}
