package qed.datauploader.sinker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import yasen.dicom.DicomWritable;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Demo {
    public static void main(String[] args) throws UnknownHostException {
//        String re = DataUploaderTool.getMD5("sdsds");
//        System.out.println(re);

    }

    public static void readSequenceFileTest(){
        //String uri = args[0];
        String hdfsPath = "hdfs://192.168.1.242:8020/yasen/soucedata/2015/12/8/1312211075619030631210116081200452295300000008825305-462894";
        String localPath = "F:\\临时文件";

        String dirname = hdfsPath.substring(hdfsPath.lastIndexOf("/")+1,hdfsPath.length());

        File localDir = new File(localPath+"\\"+dirname);
        localDir.mkdir();

        Configuration conf = new Configuration();
        Path path = new Path(hdfsPath);
        SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(path);

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf,option1);

            Text key = (Text) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            DicomWritable value = (DicomWritable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);

            long position = reader.getPosition();
            FileOutputStream fout = null;
            int count = 0;
            while (reader.next(key, value)) {

                String filepath = localPath+"\\" +dirname+"\\"+key.toString()+".IMA";
                File file = new File(filepath);
                file.createNewFile();
                fout = new FileOutputStream(file);
                System.out.println("本地路径dicom："+filepath);

                byte []data = value.getData();
                fout.write(data);
                fout.flush();
                fout.close();
                count++;
            }
            System.out.println("下载文件："+count);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
//    public boolean storeToHdfs(File seriesDir,String finalPositionPath,Configuration conf){
//        Path desPath = new Path(uploaderConf.getHdfsPath()+finalPositionPath);
//
//        SequenceFile.Writer writer = null;
//        try {
//            writer = SequenceFile.createWriter(conf,
//                    SequenceFile.Writer.file(desPath), SequenceFile.Writer.keyClass(Text.class),
//                    SequenceFile.Writer.valueClass(DicomWritable.class),
//                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        FileInputStream fin = null;
//        byte []data = null;
//        try {
//            if(seriesDir.isDirectory()){
//                /**如果是目录需要循环上传其下所有文件*/
//                for(File e : seriesDir.listFiles()) {
//                    fin = new FileInputStream(e);
//                    int available = fin.available();
//                    data = new byte[available];
//                    int length = fin.read(data);
//                    if(length!=available){
//                        return false;
//                    }
//                    Text key = new Text(e.getName());
//                    DicomWritable value = new DicomWritable(data);
//                    writer.append(key, value);
//                }
//            }else if(seriesDir.isFile()){
//                /**如果是文件只需要上传该文件*/
//                fin = new FileInputStream(seriesDir);
//                int available = fin.available();
//                data = new byte[available];
//                int length = fin.read(data);
//                if(length!=available){
//                    return false;
//                }
//                Text key = new Text(seriesDir.getName());
//                DicomWritable value = new DicomWritable(data);
//                writer.append(key, value);
//            }
//        } catch (FileNotFoundException e1) {
//            e1.printStackTrace();
//        } catch (IOException e1) {
//            e1.printStackTrace();
//        }finally {
//            try {
//                writer.close();
//            } catch (IOException e1) {
//                e1.printStackTrace();
//            }
//            IOUtils.closeStream(writer);
//        }
//        return true;
//    }
    private void transportClientTest() throws UnknownHostException {
//        byte []ip = new byte[4];
//        //"192.168.237.131"
//        ip[0] = (byte)192;
//        ip[1] = (byte)168;
//        ip[2] = (byte)237;
//        ip[3] = (byte)131;

        String ip[] = "192.168.237.131".split("\\.");
        System.out.println(ip[0]);


        Settings setting = Settings.builder()
                .put("cluster.name", "myes")//指定集群名称
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.sniff", true)//启动嗅探功能
                .build();

        /**
         * 2：创建客户端
         * 通过setting来创建，若不指定则默认链接的集群名为elasticsearch
         * 链接使用tcp协议即9300
         */
        TransportClient transportClient = new PreBuiltTransportClient(setting)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("me1"), 9300));
        List<DiscoveryNode> nodeList = transportClient.connectedNodes();
        System.out.println(nodeList.size());
        for(DiscoveryNode node : nodeList){
            System.out.println(node.getHostName());
        }
    }
}
