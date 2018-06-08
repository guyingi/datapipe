package yasen.datauploader.config;

import org.apache.hadoop.conf.Configuration;

public class ConfigFactory {
    public static Configuration getHdfsConfiguration(){
        Configuration hdfsConf = new Configuration();
//        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return hdfsConf;
    }

    public static UploaderConfiguration getUploaderConfiguration(){
        return new UploaderConfiguration();
    }

}
