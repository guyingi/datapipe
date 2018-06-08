package yasen.indexhbase.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import yasen.indexhbase.service.impl.HBaseServiceImpl;

import java.io.IOException;
import java.util.Random;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.indexhbase.main
 * @Description: ${todo}
 * @date 2018/6/6 15:37
 */
public class Main {
    static Random random = new Random();
    public static void main(String[] args) throws IOException {
        get();
//        put();
//        String rowkey = formatDigitalToNBit(generateRandonNumber(5)+"",5)+ System.currentTimeMillis()+"";
//        System.out.println(rowkey);
    }

    public static void put() throws IOException {
        Connection conn = null;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "/hbase");
        conf.set("hbase.zookeeper.quorum", "192.168.1.243");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("dicomseriesuid"));
        BufferedMutator mutator = conn.getBufferedMutator(params);
        try{
            Put put = new Put(Bytes.toBytes("sssssss"));
//            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualify), Bytes.toBytes(value));
            mutator.mutate(put);
            mutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            mutator.close();
            conn.close();
        }
    }

    public static void get(){
        Connection conn = null;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "/hbase");
        conf.set("hbase.zookeeper.quorum", "192.168.1.243");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("hbase conf:"+conf.toString());
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        HTable table = null;
        long length = 0;
        try {
            table = (HTable)conn.getTable(TableName.valueOf("dicom"));
//            ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("rowkey"));
            Scan scan = new Scan();
//            scan.setFilter(columnPrefixFilter);
            Get get = new Get(Bytes.toBytes("9746709d88a41cff61389dd39496526"));

//            ResultScanner scanner = table.getScanner(scan);
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowkey + ":" + cf + ":" + qualify + ":" + value);
            }
//            for(Result result : scanner) {
//                for (Cell cell : result.rawCells()) {
//                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
//                    String cf = Bytes.toString(CellUtil.cloneFamily(cell));
//                    String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    String value = Bytes.toString(CellUtil.cloneValue(cell));
//                    System.out.println(rowkey + ":" + cf + ":" + qualify + ":" + value);
//                }
//            }
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

    public static int generateRandonNumber(int n){
        int bound = 1;
        while(n-->1)
            bound*=10;
        int temp = 0;
        while(bound>(temp=random.nextInt(bound*10))){}
        return temp;
    }

    //n不能大于9，因为int类型位数限制
    public static String formatDigitalToNBit(String numberStr,int n){
        String result = "0000000000"+numberStr;
        result = result.substring(result.length()-n,result.length());
        return result;
    }
}
