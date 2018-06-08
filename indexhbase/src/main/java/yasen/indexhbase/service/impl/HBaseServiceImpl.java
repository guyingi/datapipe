package yasen.indexhbase.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import yasen.indexhbase.conf.SysConf;
import yasen.indexhbase.service.HBaseService;

import java.io.IOException;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.indexhbase.service.impl
 * @Description: ${todo}
 * @date 2018/6/6 16:07
 */
public class HBaseServiceImpl implements HBaseService {

    Connection conn = null;
    SysConf sysConf = null;

    public HBaseServiceImpl(){
        sysConf = new SysConf();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", sysConf.getHbaseRootdir());
        conf.set("hbase.zookeeper.quorum",sysConf.getZkQuorum());
        conf.set("hbase.zookeeper.property.clientPort", sysConf.getZkPort());
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void put(String tablename,String rowkey, String cf, String qualify,String value) throws IOException {
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename));
        BufferedMutator mutator = conn.getBufferedMutator(params);
        try{
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualify), Bytes.toBytes(value));
            mutator.mutate(put);
            mutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            mutator.close();
        }
    }

    @Override
    public void delete(String tablename, String rowkey) throws IOException {
        HTable table = null;
        long length = 0;
        try {
            table = (HTable)conn.getTable(TableName.valueOf(tablename));
            Delete delete = new Delete(rowkey.getBytes()); // 根据主键查询
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
