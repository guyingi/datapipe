package qed.datauploader.service.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import qed.datauploader.config.IndexFromHBaseConfig;
import qed.datauploader.config.UploaderConfiguration;
import qed.datauploader.consts.DataTypeEnum;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.service.ElasticSearchService;
import qed.datauploader.service.HbaseService;
import qed.datauploader.uploader.Uploader;
import qed.datauploader.tool.DataUploaderTool;
import qed.datauploader.service.IndexFromHBaseService;

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
        logger.log(Level.INFO,"开始做全量索引");
        boolean success = true;
        if(DataTypeEnum.DICOM == typeEnum){
            logger.log(Level.INFO,"类型：dicom");

            //步骤一、扫描dicom全表rowkey
            Map<String, String> map = hbaseService.scanValue(uploaderConfig.getDicomTablename(),null);

            logger.log(Level.INFO,"dicom表中扫到数据："+map.size());

            for(String rowkey : map.keySet()){
                logger.log(Level.DEBUG,"开始处理rowkey："+rowkey);
                Map<String, String> metaMap = hbaseService.get(uploaderConfig.getDicomTablename(),rowkey);

                //步骤二、 查看es中SeriesUID是否已经存在，如果存在则跳过
                String seriesUID= metaMap.get(SysConsts.SeriesUID);
                List<Map<String, Object>> maps = elasticSearchService.searchFileWithPhrase(uploaderConfig.getIndexDicom(), uploaderConfig.getTypeDicom(), SysConsts.SeriesUID, seriesUID);
                if(maps.size()>0){
                    logger.log(Level.DEBUG,"rowkey为:"+rowkey+"于elasticsearch中已经存在");
                    continue;
                }

                //步骤三：根据rowkey查询dicom表中的所有数据，格式化字段数据类型
                JSONObject metaJson = formatMeta(metaMap);

                //步骤四：存入es中
                success = SysConsts.SUCCESS == elasticSearchService.insertOne(uploaderConfig.getIndexDicom(),
                        uploaderConfig.getTypeDicom(),metaJson.getString(SysConsts.SeriesUID),metaJson);

                if(success){
                    logger.log(Level.DEBUG,"rowkey为:"+rowkey+"写入elasticsearch成功");
                }
            }

            logger.log(Level.INFO,"此次全量索引数量"+map.size());
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public int indexFromHBaseIncrement(DataTypeEnum typeEnum) {
        logger.log(Level.INFO,"开始做增量索引");
        boolean success = true;

        if(DataTypeEnum.DICOM == typeEnum){
            logger.log(Level.INFO,"类型：dicom");
            //步骤一：查询rowkey临时表，扫描得到所有rowkey
            Map<String, String> map = hbaseService.scanValue(indexFromHBaseConfig.getDcmRowkeyTablename(),
                    indexFromHBaseConfig.getDcmRowkeyTableQualify());

            if(map == null || map.size() == 0){
                logger.log(Level.INFO,"rowkey临时表未查到数据，退出增量索引操作流程");
                return SysConsts.FAILED;
            }

            //这里之所以直接使用临时rowkey表主键，是因为临时表的设计是rowkey与dicom的rowkey相同。
            Set<String> rowkeys = map.keySet();

            logger.log(Level.INFO,"rowkey临时表扫到数据:"+rowkeys.size());

            //idList中存放的是临时表rowkeytemp中的rowkey
            List<String> deleteIds = new ArrayList<String>();

            for(String rowkey : rowkeys){
                logger.log(Level.DEBUG,"开始处理rowkey："+rowkey);

                Map<String, String> metaMap = hbaseService.get(uploaderConfig.getDicomTablename(),rowkey);

                //步骤二、查看es中SeriesUID是否已经存在，如果存在则跳过
                String seriesUID= metaMap.get(SysConsts.SeriesUID);
                List<Map<String, Object>> maps = elasticSearchService.searchFileWithPhrase(uploaderConfig.getIndexDicom(), uploaderConfig.getTypeDicom(), SysConsts.SeriesUID, seriesUID);
                if(maps.size()>0){
                    logger.log(Level.DEBUG,"rowkey为:"+rowkey+"于elasticsearch中已经存在");
                    continue;
                }


                //步骤三、根据rowkey查询dicom表中的所有数据，格式化字段数据类型
                JSONObject metaJson = formatMeta(metaMap);



                //步骤四、存入es中,这里es的id传值空，让其自动生成
                success = SysConsts.SUCCESS == elasticSearchService.insertOne(uploaderConfig.getIndexDicom(),
                        uploaderConfig.getTypeDicom(),null,metaJson);

                //步骤五、索引成功则添加rowkey到待删除list
                if(success){
                    logger.log(Level.DEBUG,"rowkey为:"+rowkey+"写入elasticsearch成功");
                    deleteIds.add(rowkey);
                }
                logger.log(Level.DEBUG,"rowkey为:"+rowkey+"写入elasticsearch失败");
            }

            //步骤六：删除最初在rowkey临时表中扫描的rowkey.
            for(String id : deleteIds){
                hbaseService.delete(indexFromHBaseConfig.getDcmRowkeyTablename(),id);
            }
            logger.log(Level.INFO,"删除rowkey临时表数据:"+deleteIds.size());

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
