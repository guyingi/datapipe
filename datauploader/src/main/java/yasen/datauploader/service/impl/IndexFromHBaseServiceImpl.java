package yasen.datauploader.service.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import yasen.datauploader.config.IndexFromHBaseConfig;
import yasen.datauploader.config.UploaderConfiguration;
import yasen.datauploader.consts.DataTypeEnum;
import yasen.datauploader.consts.SysConsts;
import yasen.datauploader.service.ElasticSearchService;
import yasen.datauploader.service.HbaseService;
import yasen.datauploader.service.IndexFromHBaseService;
import yasen.datauploader.sinker.Uploader;
import yasen.datauploader.tool.DataUploaderTool;

import java.util.*;

public class IndexFromHBaseServiceImpl implements IndexFromHBaseService {
    static Logger logger = Logger.getLogger(Uploader.class.getName());

    IndexFromHBaseConfig indexFromHBaseConfig;
    UploaderConfiguration uploaderConfig;
    HbaseService hbaseService;
    ElasticSearchService elasticSearchService;
    List<String> integerFieldList;
    List<String> longFieldList;
    List<String> doubleFieldList;

    public IndexFromHBaseServiceImpl(){
        hbaseService = new HbaseServiceImpl();
        elasticSearchService = new ElasticSearchServiceImpl();
        indexFromHBaseConfig =  new IndexFromHBaseConfig();
        uploaderConfig = new UploaderConfiguration();
        integerFieldList = indexFromHBaseConfig.getIntegerDcmMetaType();
        longFieldList = indexFromHBaseConfig.getLongDcmMetaType();
        doubleFieldList = indexFromHBaseConfig.getDoubleDcmMetaType();
    }


    @Override
    public int indexFromHBaseFull(DataTypeEnum typeEnum) {

        boolean success = true;
        if(DataTypeEnum.DICOM == typeEnum){
            //得到dicom全表rowkey
            Map<String, String> map = hbaseService.scanValue(uploaderConfig.getDicomTablename(),null);

            for(String rowkey : map.keySet()){

                Map<String, String> metaMap = hbaseService.get(uploaderConfig.getDicomTablename(),rowkey);

                //步骤二：根据rowkey查询dicom表中的所有数据，格式化字段数据类型
                JSONObject metaJson = formatMeta(metaMap);

                //步骤三：存入es中
                success = SysConsts.SUCCESS == elasticSearchService.insertOne(uploaderConfig.getIndexDicom(),
                        uploaderConfig.getTypeDicom(),metaJson.getString(SysConsts.SeriesUID),metaJson);
            }

            logger.log(Level.INFO,"此次全量索引数量"+map.size());
        }

        return SysConsts.SUCCESS;
    }

    @Override
    public int indexFromHBaseIncrement(DataTypeEnum typeEnum) {
        boolean success = true;

        if(DataTypeEnum.DICOM == typeEnum){
            //步骤一：查询rowkey临时表，扫描得到所有rowkey
            Map<String, String> map = hbaseService.scanValue(indexFromHBaseConfig.getDcmRowkeyTablename(),
                    indexFromHBaseConfig.getDcmRowkeyTableQualify());

            if(map == null || map.size() == 0){
                return SysConsts.FAILED;
            }

            //这里之所以直接使用临时rowkey表主键，是因为临时表的设计是rowkey与dicom的rowkey相同。
            Set<String> rowkeys = map.keySet();

            //idList中存放的是临时表rowkeytemp中的rowkey
            List<String> deleteIds = new ArrayList<String>();

            for(String rowkey : rowkeys){
                Map<String, String> metaMap = hbaseService.get(uploaderConfig.getDicomTablename(),rowkey);

                //步骤二：根据rowkey查询dicom表中的所有数据，格式化字段数据类型
                JSONObject metaJson = formatMeta(metaMap);

                //步骤三：存入es中,这里es的id传值空，让其自动生成
                success = SysConsts.SUCCESS == elasticSearchService.insertOne(uploaderConfig.getIndexDicom(),
                        uploaderConfig.getTypeDicom(),null,metaJson);

                //如果索引成功则删除在临时表中的记录
                if(success){
                    deleteIds.add(rowkey);
                }
            }

            //步骤四：删除最初在rowkey临时表中扫描的rowkey.
            for(String id : deleteIds){
                hbaseService.delete(indexFromHBaseConfig.getDcmRowkeyTablename(),id);
            }

            logger.log(Level.INFO,"此次增量索引数量"+rowkeys.size());
        }

        return SysConsts.SUCCESS;
    }

    private JSONObject formatMeta(Map<String,String> map){
        JSONObject metaJson = new JSONObject();
        for(Map.Entry<String,String> entry : map.entrySet()){
            String field = entry.getKey();
            String value = entry.getValue();
            if(!StringUtils.isBlank(value)){
                if(integerFieldList.contains(field))
                    metaJson.put(field,DataUploaderTool.formatInteger(value));
                else if(longFieldList.contains(field)){
                    metaJson.put(field,DataUploaderTool.formatLong(value));
                }else if(doubleFieldList.contains(field)){
                    metaJson.put(field,DataUploaderTool.formatDouble(value));
                }else {
                    metaJson.put(field, value);
                }
            }
        }
        return metaJson;
    }

}
