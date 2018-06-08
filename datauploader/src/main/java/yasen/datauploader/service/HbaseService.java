package yasen.datauploader.service;

import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface HbaseService {
    /**插入单条数据：参数：表名，列簇，列，值 */
    int putOne(String tableName, String cf, JSONObject metaJson) throws IOException;

    /**插入单条数据：参数：表名，列簇，列，值 */
    int putOne(String tableName, String cf, Map<String,String> metaMap) throws IOException;

    /**插入多条数据：参数：表名，列簇，List<列，值>*/
    int putBatch(String tableName, String cf, Map<String,String> colvalues);

    /**插入一个单元格**/
    int putCell(String tableName,String rowkey, String cf,String col,byte[] value) throws IOException;

    /**删除单条数据，根据rowkey*/
    boolean delete(String tableName, String rowkey);

    boolean isExists(String tableName,String rowkey) throws IOException;

    /**上传缩略图，dir为dicom目录，metaJson为包含所有元数据以及rowkey的对象*/
    int uploadThumbnail(File dir, String dcmrowkey,List<String> thumbnailRowkeyList);

    /**
     * 该方法扫描表，得到rowkey,及某个指定列,如果qualify为null,空串，则只返回全表rowkey
     * @param tablename
     * @param qualify
     * @return
     */
    Map<String,String> scanValue(String tablename,String qualify);

    Map<String,String> get(String tablename,String rowkey);
}

