package yasen.datauploader.service;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;

public interface ElasticSearchService {
    int insertOne(String index,String type,String id,JSONObject metaMsg);
    int insertOne(String index,String type,String id,Map<String,String> metaMsg);
    int insertBatch(List<JSONObject> metaMsgs);
    boolean delete(JSONObject metaMsg,String index,String type);
    boolean isExists(String id);

    Object getField(String index,String type,String id,String field);

}
