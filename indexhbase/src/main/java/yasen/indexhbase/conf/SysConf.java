package yasen.indexhbase.conf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.indexhbase.conf
 * @Description: ${todo}
 * @date 2018/6/6 17:25
 */
public class SysConf {

    private String hbaseRootdir;
    private String zkQuorum;
    private String zkPort;

    private String rowkeyTablename;
    private String rowkeyTableCf;
    private String rowkeyTableQualify;


    public SysConf(){
        InputStreamReader reader = new InputStreamReader(SysConf.class.getClassLoader().getResourceAsStream("indexhbase.properties"));
        Properties props = new Properties();
        try {
            props.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hbaseRootdir = props.getProperty("hbase.rootdir");
        zkQuorum = props.getProperty("hbase.zookeeper.quorum");
        zkPort = props.getProperty("hbase.zookeeper.property.clientPort");

        rowkeyTablename = props.getProperty("name.rowkey.tablename");
        rowkeyTableCf = props.getProperty("name.rowkey.cf");
        rowkeyTableQualify = props.getProperty("name.rowkey.qualify");
    }

    public String getHbaseRootdir() {
        return hbaseRootdir;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getZkPort() {
        return zkPort;
    }

    public String getRowkeyTablename() {
        return rowkeyTablename;
    }

    public String getRowkeyTableCf() {
        return rowkeyTableCf;
    }

    public String getRowkeyTableQualify() {
        return rowkeyTableQualify;
    }
}
