package qed.datauploader.service.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import qed.datauploader.service.ElasticSearchService;
import qed.datauploader.consts.SysConsts;
import qed.datauploader.uploader.Uploader;
import qed.datauploader.factory.EsClientFactory;
import qed.datauploader.config.UploaderConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchServiceImpl implements ElasticSearchService {
    static Logger logger = Logger.getLogger(Uploader.class.getName());

    UploaderConfiguration uploaderConf = null;
    TransportClient transportClient = null;
    public ElasticSearchServiceImpl(){
        init();
    }

    private void init(){
        this.uploaderConf = new UploaderConfiguration();
        this.transportClient = EsClientFactory.getTransportClient();
    }

    @Override
    public int insertOne(String index,String type,String id,JSONObject docJson) {
        if(index==null||type==null){
            return SysConsts.FAILED;
        }

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        Map resultMap = new HashMap();

        //没有指定idName 那就让Elasticsearch自动生成
        if(StringUtils.isBlank(id)){
//            if(isExists(idValue)){ 这里其实不需要判断是否已经存在，如果存在ES默认是替换旧的
//                return SysConsts.EXISTS;
//            }
            IndexRequestBuilder lrb = transportClient.prepareIndex(index, type,id).setSource(docJson);
            bulkRequest.add(lrb);
        }else{
//            return SysConsts.FAILED;
            IndexRequestBuilder lrb = transportClient.prepareIndex(index, type).setSource(docJson);
            bulkRequest.add(lrb);
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            System.out.println("message"+bulkResponse.buildFailureMessage());
            return SysConsts.FAILED;
        }
        return SysConsts.SUCCESS;
    }

    @Override
    public int insertOne(String index, String type, String id, Map<String, String> metaMsg) {
        JSONObject metaJSON = new JSONObject();
        for(Map.Entry<String,String> entry : metaMsg.entrySet()){
            metaJSON.put(entry.getKey(),entry.getValue());
        }
        return insertOne(index, type, id, metaJSON);
    }

    @Override
    public int insertBatch(List<JSONObject> metaMsgs) {
        return 0;
    }

    @Override
    public boolean delete(JSONObject metaMsg,String index,String type) {
        if(index==null||type==null){
            return false;
        }
        String id  = metaMsg.getString(SysConsts.SeriesUID);
        DeleteResponse deleteResponse = transportClient.prepareDelete(index, type, id).execute().actionGet();
        return true;
    }

    @Override
    public boolean isExists(String id) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery(uploaderConf.getTypeDicom());
        idsQueryBuilder.addIds(id);
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(uploaderConf.getIndexDicom())
                .setTypes(uploaderConf.getTypeDicom())
                .setQuery(idsQueryBuilder)
                .setSearchType(SearchType.DEFAULT)
                .setScroll(new TimeValue(100000))
                .setSize(1000)
                // 设置是否按查询匹配度排序
                .setExplain(false);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        long totalHits = response.getHits().getTotalHits();
        return totalHits>0;
    }

    @Override
    public Object getField(String index, String type, String id, String field) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery(type);
        idsQueryBuilder.addIds(id);

        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index)
                .setTypes(type)
                .setQuery(idsQueryBuilder)
                .setSearchType(SearchType.DEFAULT)
                .setSize(1000)
                // 设置是否按查询匹配度排序
                .setExplain(false);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        long len = response.getHits().getTotalHits();
        for(SearchHit hit : response.getHits().getHits()){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Object fieldValue = sourceAsMap.get(field);
            return (String)fieldValue;
        }
        return null;
    }

    @Override
    public List<Map<String,Object>> searchFileWithPhrase(String index, String type, String fieldname,String fieldvlaue) {
        List<Map<String,Object>> result = new ArrayList<>();
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery(fieldname, fieldvlaue);
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index)
                .setTypes(type)
                .setQuery(matchPhraseQueryBuilder)
                .setSearchType(SearchType.DEFAULT)
                .setSize(1000)
                // 设置是否按查询匹配度排序
                .setExplain(false);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        for(SearchHit hit : response.getHits().getHits()){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            result.add(sourceAsMap);
        }
        return result;
    }
}
