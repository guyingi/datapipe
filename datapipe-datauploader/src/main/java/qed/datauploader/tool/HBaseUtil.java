package qed.datauploader.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.log4j.Logger;
import qed.datauploader.config.UploaderConfiguration;

import java.io.IOException;

public class HBaseUtil {
    static Logger logger = Logger.getLogger(HBaseUtil.class.getName());
    static UploaderConfiguration uploaderConfiguration = null;
    static BufferedMutator.ExceptionListener listener = null;
    static Connection conn = null;
    static Configuration conf = HBaseConfiguration.create();

    static{
        uploaderConfiguration = new UploaderConfiguration();
        conf.set("hbase.rootdir", "/hbase");
        conf.set("hbase.zookeeper.quorum", uploaderConfiguration.getZookeeperQuorum());
        conf.set("hbase.zookeeper.property.clientPort", uploaderConfiguration.getZookeeperClientPort());
        System.out.println("hbase conf:"+conf.toString());
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
    }

    public static Connection getConnection(){
        return conn;
    }
}
