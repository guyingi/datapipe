package qed.datauploader.test;//package test;
//
//import com.google.gson.JsonObject;
//import com.google.gson.JsonPrimitive;
//import org.apache.http.HttpHost;
//import org.codehaus.jettison.json.JSONException;
//import org.codehaus.jettison.json.JSONObject;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchRequestBuilder;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.index.query.*;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.elasticsearch.search.sort.SortOrder;
//import tool.SysConsts;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;
//
//public class EsSearchTool {
//    public static void yasen.indexhbase.main(String[] args) throws IOException {
//        JsonObject json = new JsonObject();
//        boolean flag = isExists("12840113619236314770990321484028207253990");
//        System.out.println(flag);
////        String hos = "PUMCH";
////        String se = "M";
////        String seq = "1.2.840.113619.2.363.10499743.3902406.15735.1483921213.783";
////        json.addProperty(SysConsts.HOSPITAL,hos);
////        json.addProperty(SysConsts.SEX,se);
////        json.addProperty(SysConsts.MRI_SEQUENCE,seq);
////        List<String> result = esSearch(json);
////        for(String e : result){
////            System.out.println(e);
////        }
//    }
//
//    public void BoolSearch(TransportClient client){
//        //多条件设置
//        MatchPhraseQueryBuilder mpq1 = QueryBuilders
//                .matchPhraseQuery("pointid","W3.UNIT1.10LBG01CP301");
//        MatchPhraseQueryBuilder mpq2 = QueryBuilders
//                .matchPhraseQuery("inputtime","2016-07-21 00:00:01");
//        QueryBuilder qb2 = QueryBuilders.boolQuery()
//                .must(mpq1)
//                .must(mpq2);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.query(qb2);
//        //System.out.println(sourceBuilder.toString());
//
//        //查询建立
//        SearchRequestBuilder responsebuilder = client
//                .prepareSearch("pointdata").setTypes("pointdata");
//        SearchResponse myresponse=responsebuilder
//                .setQuery(qb2)
//                .setFrom(0).setSize(50)
//                .addSort("inputtime", SortOrder.ASC)
//                //.addSort("inputtime", SortOrder.DESC)
//                .setExplain(true).execute().actionGet();
//        SearchHits hits = myresponse.getHits();
//        for(int i = 0; i < hits.getHits().length; i++) {
//            System.out.println(hits.getHits()[i].getSourceAsString());
//
//        }
//    }
//
//
//    public static List<String> doSearch(SearchSourceBuilder searchSourceBuilder){
//        List<String> resultList = new LinkedList<String>();
//        SearchResponse searchResponse = null;
//        SearchHits hits;
//        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
//                new HttpHost("192.168.237.131", 9200, "http")))) {
//            SearchRequest searchRequest = new SearchRequest();
////        searchRequest.indices("social-*");
//            searchRequest.source(searchSourceBuilder);
//            searchResponse = client.search(searchRequest);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        hits = searchResponse.getHits();
//        for(int i = 0; i < hits.getHits().length; i++) {
//            String result = hits.getHits()[i].getSourceAsString();
//            try {
//                JSONObject jsonObject = new JSONObject(result);
//                resultList.add(jsonObject.getString("filepath"));
//            } catch (JSONException e) {
//                e.printStackTrace();
//            }
//        }
//        return resultList;
//    }
//
//    public static List<String> esSearch(JsonObject searchFieldJson){
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//
//        List<QueryBuilder> matchQueryList = new ArrayList<QueryBuilder>();
//        //创建查询条件
//        //精确值的字段：MRI序列，性别
//        //如果有该查询条件则加到querybuilder中，否则则不加
//        JsonPrimitive mriSeq = searchFieldJson.getAsJsonPrimitive(SysConsts.MRISEQ);
//        if(mriSeq != null && mriSeq.toString().length()!=0){
//            //此处使用termsQuery(String name,String value)精确匹配单个值
//            matchQueryList.add(QueryBuilders.matchQuery(SysConsts.MRISEQ, mriSeq.toString()));
//        }
//        JsonPrimitive sex = searchFieldJson.getAsJsonPrimitive(SysConsts.SEX);
//        if(sex != null && sex.toString().length()!=0){
//            //此处使用termsQuery(String name,String value)精确匹配单个值
//            matchQueryList.add(QueryBuilders.matchQuery(SysConsts.SEX, sex.toString()));
//            System.out.println(sex.toString());
//        }
//
//        //范围值的字段：年龄段，检查日期段，数据收入日期段
//        // 查询在时间区间范围内的结果
//        //年龄段
//        JsonPrimitive ageSection = searchFieldJson.getAsJsonPrimitive(SysConsts.AGE_SECTION);
//        if(ageSection != null && ageSection.toString().length()!=0){
//            RangeQueryBuilder rangbuilder = QueryBuilders.rangeQuery(SysConsts.AGE);
//            String section[] = ageSection.toString().split("\\-");
//            rangbuilder.gte(section[0]);
//            rangbuilder.lte(section[1]);
//            matchQueryList.add(rangbuilder);
//        }
//        //检查日期段20170811-20180201
//        JsonPrimitive studyDateSection = searchFieldJson.getAsJsonPrimitive(SysConsts.STUDYDATE_SECTION);
//        if(studyDateSection != null && studyDateSection.toString().length()!=0){
//            RangeQueryBuilder rangbuilder = QueryBuilders.rangeQuery(SysConsts.STUDYDATE);
//            String section[] = studyDateSection.toString().split("\\-");
//            rangbuilder.gte(section[0]);
//            rangbuilder.lte(section[1]);
//            matchQueryList.add(rangbuilder);
//        }
//        //数据收入日期段
//        JsonPrimitive entryDateSection = searchFieldJson.getAsJsonPrimitive(SysConsts.ENTRYDATE_SECTION);
//        if(entryDateSection !=null && entryDateSection.toString().length()!=0){
//            RangeQueryBuilder rangbuilder = QueryBuilders.rangeQuery(SysConsts.ENTRYDATE);
//            String section[] = entryDateSection.toString().split("\\-");
//            rangbuilder.gte(section[0]);
//            rangbuilder.lte(section[1]);
//            matchQueryList.add(rangbuilder);
//        }
//
//        //枚举值的字段：设备种类，数据来源（医院），扫描部位
//
//        JsonPrimitive deviceType = searchFieldJson.getAsJsonPrimitive(SysConsts.DEVIETYPE);
//        if(deviceType != null && deviceType.toString().length()!=0){
//            //此处使用termsQuery(String name,String ...values)同时匹配多个值
//            matchQueryList.add(QueryBuilders.matchQuery(SysConsts.DEVIETYPE,deviceType.toString() ));
//        }
//        JsonPrimitive hospital = searchFieldJson.getAsJsonPrimitive(SysConsts.HOSPITAL);
//        if(hospital != null && hospital.toString().length()!=0){
//            //此处使用termsQuery(String name,String ...values)同时匹配多个值
//            matchQueryList.add(QueryBuilders.matchQuery(SysConsts.HOSPITAL, hospital.toString()));
//        }
//        JsonPrimitive organ = searchFieldJson.getAsJsonPrimitive(SysConsts.ORGAN);
//        if(organ != null && organ.toString().length()!=0){
//            //此处使用termsQuery(String name,String ...values)同时匹配多个值
//            matchQueryList.add(QueryBuilders.matchQuery(SysConsts.ORGAN, organ.toString()));
//        }
//
//        // 等同于bool，将两个查询合并
//        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
//        int i = 0;
//        for(QueryBuilder e : matchQueryList) {
//            i++;
//            boolBuilder.must(e);
//        }
//        System.out.println(i);
//
//        searchSourceBuilder.query(boolBuilder);
////        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
////        searchSourceBuilder.aggregation(AggregationBuilders.terms("top_10_states").field("state").size(10));
//        return doSearch(searchSourceBuilder);
//    }
//
//    public static boolean isExists(String id){
//        JsonObject json = new JsonObject();
//        json.addProperty(SysConsts.ID,id);
//        List<String> results = esSearch(json);
//        return results.size()!=0;
//    }
//
//}
//
