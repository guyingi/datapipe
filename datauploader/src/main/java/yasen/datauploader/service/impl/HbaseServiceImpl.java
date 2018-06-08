package yasen.datauploader.service.impl;

import bsh.StringUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import yasen.datauploader.config.UploaderConfiguration;
import yasen.datauploader.consts.SysConsts;
import yasen.datauploader.factory.ThumbnailServiceFactory;
import yasen.datauploader.service.HbaseService;
import yasen.datauploader.service.ThumbnailService;
import yasen.datauploader.sinker.Uploader;
import yasen.datauploader.tool.DataUploaderTool;
import yasen.datauploader.tool.HBaseUtil;
import yasen.datauploader.tool.IMAParseTool;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class HbaseServiceImpl implements HbaseService {
    static Logger logger = Logger.getLogger(Uploader.class.getName());
    static UploaderConfiguration uploaderConf = new UploaderConfiguration();
    static BufferedMutator.ExceptionListener listener = null;
    static Connection conn = null;
    static List<String> IntegerFieldList = new ArrayList<String>();
    static List<String> LongFieldList = new ArrayList<String>();
    static List<String> DoubleFieldList = new ArrayList<String>();

    public HbaseServiceImpl(){
    }

    static{
        conn = HBaseUtil.getConnection();
        listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };

//        IntegerFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientAge_TAG));
//        IntegerFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.SeriesNumber_TAG));
//        IntegerFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.NumberOfSlices_TAG));
//
//        LongFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.StudyTime_TAG));
//        LongFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.SeriesTime_TAG));
//        LongFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.AcquisitionTime_TAG));
//        LongFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.ContentTime_TAG));
//
//        DoubleFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientSize_TAG));
//        DoubleFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.PatientWeight_TAG));
//        DoubleFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.SliceThickness_TAG));
//        DoubleFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.ReconstructionDiameter_TAG));
//        DoubleFieldList.add(SysConsts.DCM_META_TAG2KW.get(SysConsts.SliceLocation_TAG));
    }

    @Override
    public int putOne(String tableName, String cf, JSONObject metaJson) throws IOException {
        String rowkey = metaJson.getString(SysConsts.ROWKEY);
        if(isExists(tableName,rowkey)){
            return SysConsts.EXISTS;
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(listener);
        try(BufferedMutator mutator = conn.getBufferedMutator(params)){
            Put put = new Put(Bytes.toBytes(rowkey));
            for(String key : metaJson.keySet()){
                String value = metaJson.getString(key);
                value = value == null ? "" : value;
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(value));
            }
            mutator.mutate(put);
            mutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public int putOne(String tableName, String cf, Map<String, String> metaMap) throws IOException {
        JSONObject metaJSON = new JSONObject();
        for(Map.Entry<String,String> entry : metaMap.entrySet()){
            metaJSON.put(entry.getKey(),entry.getValue());
        }
        return putOne(tableName, cf,metaJSON);
    }

    @Override
    public int putBatch(String tableName, String cf, Map<String, String> colvalues) {
        return 0;
    }

    @Override
    public int putCell(String tableName, String rowkey, String cf, String col, byte[] value) throws IOException {
        if(StringUtils.isBlank(rowkey) || StringUtils.isBlank(cf)|| StringUtils.isBlank(col) || value==null){
            return SysConsts.FAILED;
        }
        if(isExists(tableName,rowkey)){
            return SysConsts.EXISTS;
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        try(BufferedMutator mutator = conn.getBufferedMutator(params)){
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(col),value);
            mutator.mutate(put);
            mutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
            return SysConsts.FAILED;
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public boolean delete(String tableName, String rowkey) {
        HTable table = null;
        try {
            table = (HTable)conn.getTable(TableName.valueOf(tableName));
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
        return true;
    }

    /**
     * 这里需要查SeriesUID的重复情况
     * @param tableName
     * @param rowkey
     * @return
     */
    @Override
    public boolean isExists(String tableName, String rowkey){
        HTable table = null;
        long length = 0;
        try {
            table = (HTable)conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowkey.getBytes()); // 根据主键查询
            Result result = table.get(get);
            length = result.rawCells().length;
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return length>0;
    }

    @Override
    public int uploadThumbnail(File dir, String dcmrowkey,List<String> thumbnailRowkeyList) {

        if(dir == null || StringUtils.isBlank(dcmrowkey) || thumbnailRowkeyList == null){
            System.out.println("uploadThumbnail中存在参数为空");
            return SysConsts.FAILED;
        }

        ThumbnailService thumbnailService = ThumbnailServiceFactory.getThumbnailService();
        Set<String> rowkeySet = new HashSet<String>();
        //获取图片序号，ImageNumber
        String seqNo;
        for(File e : dir.listFiles()){
            JSONObject field = IMAParseTool.extractExactField(e.getAbsolutePath(),SysConsts.InstanceNumber_TAG);
            if(!field.isEmpty()){
                String imageNumberStr = field.getString(SysConsts.InstanceNumber_TAG);
                seqNo = DataUploaderTool.formatDigitalToNBit(imageNumberStr, 6);
                //生成rowkey
                String rowkey = dcmrowkey+seqNo;
                thumbnailRowkeyList.add(rowkey);

                //获取图片字节数组
                byte[] thumbnailValue = thumbnailService.createThumbnail(e.getAbsolutePath());
                if( thumbnailValue == null || thumbnailValue.length == 0 ){
                    return SysConsts.FAILED;
                }

                try {
                    //存入hbase
                   if(SysConsts.FAILED==putCell(uploaderConf.getDicomThumbnailTablename(),
                            rowkey,uploaderConf.getDicomThumbnailCf(),SysConsts.THUMBNAIL,thumbnailValue)){
                       //如果有一个插入失败，回滚前面插入的缩略图
                       for(String key : rowkeySet){
                           delete(uploaderConf.getDicomTablename(),key);
                       }
                       return SysConsts.FAILED;
                   }
                } catch (IOException e1) {
                    e1.printStackTrace();
                    return SysConsts.FAILED;
                }
                rowkeySet.add(rowkey);
            }
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public Map<String, String> scanValue(String tablename,String qualify) {
        if(StringUtils.isBlank(tablename)){
            return null;
        }
        Map<String,String> map = new HashMap<String,String>();
        HTable table;
        try {
            table = (HTable)conn.getTable(TableName.valueOf(tablename));
            Scan scan = new Scan();
            if(!StringUtils.isBlank(qualify)) {
                ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(qualify));
                scan.setFilter(columnPrefixFilter);
            }
            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                String rowkey = Bytes.toString(result.getRow());
                if(!StringUtils.isBlank(qualify)) {
                    for (Cell cell : result.rawCells()) {
                        if (qualify.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            map.put(rowkey, value);
                            continue;
                        }
                    }
                }else {
                    map.put(rowkey, null);
                }
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    @Override
    public Map<String, String> get(String tablename,String rowkey) {
        if(StringUtils.isBlank(tablename) || StringUtils.isBlank(rowkey)){
            return null;
        }
        Map<String,String> map = new HashMap<String,String>();
        HTable table = null;
        try {
            table = (HTable)conn.getTable(TableName.valueOf(tablename));
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for(Cell cell : result.rawCells()){
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                map.put(qualifier,value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
