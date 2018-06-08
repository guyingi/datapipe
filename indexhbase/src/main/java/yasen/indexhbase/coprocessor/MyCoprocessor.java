package yasen.indexhbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import yasen.indexhbase.conf.SysConf;
import yasen.indexhbase.service.impl.HBaseServiceImpl;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package coprocessor
 * @Description: ${todo}
 * @date 2018/6/6 15:33
 */
public class MyCoprocessor extends BaseRegionObserver {

    HBaseServiceImpl hBaseService = null;

    Random rand  = new Random();
    SysConf sysConf = null;
    String rowkeyTablename = null;
    String rowkeyTableCf = null;
    String rowkeyTableQualify = null;
    public MyCoprocessor(){
        sysConf = new SysConf();
        rowkeyTablename = sysConf.getRowkeyTablename();
        rowkeyTableCf = sysConf.getRowkeyTableCf();
        rowkeyTableQualify = sysConf.getRowkeyTableQualify();
        hBaseService = new HBaseServiceImpl();
    }

    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(put.getRow());
        hBaseService.put(rowkeyTablename,rowkey,rowkeyTableCf,rowkeyTableQualify,rowkey);
    }

    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(delete.getRow());
        hBaseService.delete(rowkeyTablename,rowkey);
    }


        /**
         * 生成n位随机数
         * @return
         */
    public int generateRandonNumber(int n){
        int bound = 1;
        while(n-->1)
            bound*=10;
        int temp = 0;
        while(bound>(temp=rand.nextInt(bound*10))){}
        return temp;
    }

    //n不能大于9，因为int类型位数限制
    public static String formatDigitalToNBit(String numberStr,int n){
        String result = "0000000000"+numberStr;
        result = result.substring(result.length()-n,result.length());
        return result;
    }

}
