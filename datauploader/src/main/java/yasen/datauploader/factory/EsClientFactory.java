package yasen.datauploader.factory;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import yasen.datauploader.config.UploaderConfiguration;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EsClientFactory {

    private EsClientFactory(){}

    public static TransportClient getTransportClient(){
        UploaderConfiguration uploaderConf = new UploaderConfiguration();
        Settings setting = Settings.builder()
                .put("cluster.name", uploaderConf.getEscluster())//指定集群名称
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.sniff", true)//启动嗅探功能
                .build();

        /**
         * 2：创建客户端
         * 通过setting来创建，若不指定则默认链接的集群名为elasticsearch
         * 链接使用tcp协议即9300
         */
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
        return  new PreBuiltTransportClient(setting)
                .addTransportAddress(new TransportAddress(address,Integer.parseInt(uploaderConf.getEsport())));
    }
}
