package yasen.datauploader.tool;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESTool {

   /* static TransportClient transportClient = null;
    static String index = null;
    static String type = null;

    static{

       //   1:通过 setting对象来指定集群配置信息
        UploaderConfiguration uploaderConf = new UploaderConfiguration();
        index = uploaderConf.getIndex();
        type = uploaderConf.getType();
        Settings setting = Settings.builder()
                .put("cluster.name", uploaderConf.getEscluster())//指定集群名称
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.sniff", true)//启动嗅探功能
                .build();


//          2：创建客户端
//          通过setting来创建，若不指定则默认链接的集群名为elasticsearch
//          链接使用tcp协议即9300
        InetAddress address = null;
        if(uploaderConf.getEsip()!=null){
            String ip[] = uploaderConf.getEsip().split("\\.");
            byte bip[] = new byte[4];
            bip[0] = (byte)Integer.parseInt(ip[0]);
            bip[1] = (byte)Integer.parseInt(ip[1]);
            bip[2] = (byte)Integer.parseInt(ip[2]);
            bip[3] = (byte)Integer.parseInt(ip[3]);
            try {
                address = InetAddress.getByAddress(bip);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }else if(uploaderConf.getEshost()!=null){
            try {
                address = InetAddress.getByName(uploaderConf.getEshost());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }else{

        }
        transportClient = new PreBuiltTransportClient(setting)
                    .addTransportAddress(new TransportAddress(address,Integer.parseInt(uploaderConf.getEsport())));
        List<DiscoveryNode> nodeList = transportClient.connectedNodes();
        for(DiscoveryNode node : nodeList){
            System.out.println(node.getHostName());
        }
    }*/

   /* public static boolean storeSingleToES(JSONObject metaMsg) {
        if(index==null||type==null){
            return false;
        }

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        Map resultMap = new HashMap();

        String idValue = metaMsg.getString(SysConsts.ID);
        //没有指定idName 那就让Elasticsearch自动生成
        if(idValue!=null&&idValue.length()!=0){
            IndexRequestBuilder lrb = transportClient.prepareIndex(index, type,idValue).setSource(metaMsg);
            bulkRequest.add(lrb);
        }else{
            IndexRequestBuilder lrb = transportClient.prepareIndex(index, type).setSource(metaMsg);
            bulkRequest.add(lrb);
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            System.out.println("message"+bulkResponse.buildFailureMessage());
            return false;
        }
        return true;
    }*/

   /* public static boolean storePatchToES(List<JSONObject> jsonList) {
        if(index==null||type==null){
            return false;
        }
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        Map resultMap = new HashMap();


        for (JSONObject json : jsonList) {
            String idName = json.getString(SysConsts.ID);
            //没有指定idName 那就让Elasticsearch自动生成
            if(idName!=null&&idName.length()!=0){
                String idValue = json.getString(SysConsts.ID);
                IndexRequestBuilder lrb = transportClient.prepareIndex(index, type,idValue).setSource(json);
                bulkRequest.add(lrb);
            }else{
                IndexRequestBuilder lrb = transportClient.prepareIndex(index, type).setSource(json);
                bulkRequest.add(lrb);
            }
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            return false;
        }
        return true;
    }*/


//
//    /******插入单条数据到ES,方法为java通过http调用rest api,方法很Low,估计会被遗弃*****/
//    public static boolean storeSingleToES(JsonObject json, UploaderConfiguration conf){
////        String source = "{\"0008,0012\":\"20170110\",\"0008,0018\":\"1.2.840.113619.2.363.1477099032.1484028207.274891\",\"0010,0010\":\"BA TAO\",\"0010,0020\":\"40489341\",\"0010,1010\":\"014Y\",\"0020,0011\":\"3\",\"0020,0013\":\"86\",\"0054,0081\":\"89\"}";
////        String url = "http://192.168.237.131:9200/is/you";
////        String url = "http://192.168.237.131:9200/is/you/3/";
//
//        try {
//            byte[] param = json.toString().getBytes("UTF-8");
//            String id = json.get(SinkConstant.MRISEQ).toString();
//            id = id.substring(1,id.length()-1).replace(".","");
//            if(EsSearchTool.isExists(id)){
//                return true;
//            }
//            String url = conf.getEsurl() + conf.getIndexAndType() + id;
////            System.out.println("====================="+json.get(SinkConstant.MRISEQ));
//            URL restServiceURL = new URL(url);
//
//            HttpURLConnection httpConnection = (HttpURLConnection) restServiceURL.openConnection();
//            httpConnection.setRequestMethod("POST");
//            httpConnection.setRequestProperty("Accept", "application/json");
//            httpConnection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
//            //        httpConnection.setRequestProperty("Connection", "Keep-Alive");
//            httpConnection.setRequestProperty("Charset", "UTF-8");
//            httpConnection.setDoOutput(true);
//            httpConnection.setDoInput(true);
//
//            //传递参数
//            httpConnection.setRequestProperty("Content-Length", String.valueOf(param));
//            OutputStream outputStream = httpConnection.getOutputStream();
//            outputStream.write(param);
//            outputStream.flush();
//            outputStream.close();
//            if (httpConnection.getResponseCode() == 201) {
//                httpConnection.disconnect();
//                return true;
//            } else {
//                httpConnection.disconnect();
//                throw new RuntimeException("HTTP POST Request Failed with Error code : " + httpConnection.getResponseCode());
//            }
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//        return false;
//    }
}
