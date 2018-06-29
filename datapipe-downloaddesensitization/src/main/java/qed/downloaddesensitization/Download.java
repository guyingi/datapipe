package qed.downloaddesensitization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package qed.downloaddesensitization
 * @Description: ${todo}
 * @date 2018/6/25 19:29
 */
public class Download {
    public static Logger logger = Logger.getLogger(Main.class);

    public void doDownload(String jsonFilePath,String outputPath){

        logger.log(Level.INFO,"方法doDownload接收参数:{jsonFilePath:"+jsonFilePath+",outputPath:"+outputPath);

        //解析json
        String path = jsonFilePath;
        JSONObject data = parseJSON(path);

        //准备临时目录/temp/download/AA
        String downlaodPath = Tool.getRunnerPath()+File.separator+"temp";
        if(new File(downlaodPath).exists()){
            Tool.delAllFile(downlaodPath);
        }else{
            new File(downlaodPath).mkdirs();
        }
        logger.log(Level.INFO,"临时目录:"+downlaodPath);

        //准备临时目录/temp/result/AA
        String lastResultPath = outputPath;
        if(new File(lastResultPath).exists()){
            Tool.delAllFile(lastResultPath);
        }else{
            new File(lastResultPath).mkdirs();
        }
        logger.log(Level.INFO,"结果目录:"+lastResultPath);

        //下载到本地
        //以tag为单位逐个处理
        Set<String> keySet = data.keySet();
        for(String tagname : keySet){
            logger.log(Level.INFO,"处理tag:"+tagname);
            //map中存储的是同一个tag中的study->List<序列>这种关系的数据
            List<String> hdfspaths = new ArrayList<String>();
            Map<String,String> map = new HashMap<String, String>();

            JSONArray jsonArray = data.getJSONArray(tagname);
            int size = jsonArray.size();
            for(int i=0;i<size;i++){
                JSONObject obj = jsonArray.getJSONObject(i);
                String hdfspath = obj.getString("hdfspath");
                String organ = obj.getString("organ");
                map.put(hdfspath,organ);
                hdfspaths.add(hdfspath);
            }

            //准备路径
            //最开始文件就下载到这个目录下面
            String downlaodTagPath = downlaodPath+File.separator+tagname;

            Configuration config = new Configuration();
            config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            for(String hdfspath : hdfspaths){
                logger.log(Level.INFO,"下载:"+hdfspath);
                Tool.copyDirToLocal(hdfspath,downlaodTagPath,config);
            }

            //删除hdfs api产生的crc校验文件，
            for(File seriesFile : new File(downlaodTagPath).listFiles()){
                for(File e : seriesFile.listFiles()){
                    if(e.getName().endsWith("crc")){
                        e.delete();
                    }
                }
            }

            String lastResultTagPath = lastResultPath+File.separator+tagname;

            //遍历map
            int seq = 1;
            for(Map.Entry<String,String> entry : map.entrySet()){
                String hdfspath = entry.getKey();
                String organ = entry.getValue();

                String seriseDirName = hdfspath.substring(hdfspath.lastIndexOf("/"),hdfspath.length());
                String seriesDirPath = downlaodTagPath+File.separator+seriseDirName;        //这个序列的目录，下面可能是四个乳腺，也可能是一个肺
                if("breast".equals(organ)){
                    String prefixName = tagname+"_"+Tool.formatDigitalToNBit(seq+"",6)+"_";

                    Set<String> oldNameSet = new HashSet<String>();
                    for(File file : new File(seriesDirPath).listFiles()){
                        String name = file.getName();
                        if(name.endsWith(".mhd")){
                            oldNameSet.add(name.substring(0,name.indexOf(".")));
                        }
                    }
                    for(String oldName : oldNameSet){
                        String newName = prefixName+oldName;
                        rename(oldName,newName,seriesDirPath);
                    }
                    //改info.csv,roi.csv的名字
                    String infocsvOldname = seriesDirPath+File.separator+"info.csv";
                    String infocsvNewname = seriesDirPath+File.separator+prefixName+"info.csv";
                    String roicsvOldname = seriesDirPath+File.separator+"ROI.csv";
                    String roicsvNewname = seriesDirPath+File.separator+prefixName+"ROI.csv";
                    new File(infocsvOldname).renameTo(new File(infocsvNewname));
                    new File(roicsvOldname).renameTo(new File(roicsvNewname));
                }else if("lung".equals(organ)){
                    String prefixName = tagname+"_"+Tool.formatDigitalToNBit(seq+"",6);

                    Set<String> oldNameSet = new HashSet<String>();
                    for(File file : new File(seriesDirPath).listFiles()){
                        String name = file.getName();
                        if(name.endsWith(".mhd")){
                            oldNameSet.add(name.substring(0,name.indexOf(".")));
                        }
                    }
                    for(String oldName : oldNameSet){
                        String newName = prefixName;
                        rename(oldName,newName,seriesDirPath);
                    }
                    //改info.csv,roi.csv的名字
                    String infocsvOldname = seriesDirPath+File.separator+"info.csv";
                    String infocsvNewname = seriesDirPath+File.separator+prefixName+"_"+"info.csv";
                    String roicsvOldname = seriesDirPath+File.separator+"ROI.csv";
                    String roicsvNewname = seriesDirPath+File.separator+prefixName+"_"+"ROI.csv";
                    new File(infocsvOldname).renameTo(new File(infocsvNewname));
                    new File(roicsvOldname).renameTo(new File(roicsvNewname));
                }
                seq++;
            }

            logger.log(Level.INFO,"拷贝到最终目录:"+lastResultTagPath);
            for(File file : new File(downlaodTagPath).listFiles()){
                Tool.copyDir(file.getAbsolutePath(),lastResultTagPath);
            }
            logger.log(Level.INFO,"删除临时目录:"+downlaodTagPath);
            Tool.delFolder(downlaodTagPath);
        }
        logger.log(Level.INFO,"方法doDownload调用结束");
    }

    /**
     *
     * @param oldname raw,mhd这样的数据对名字
     * @param newname 新的名字
     * @param path  这个目录下面存放着raw,mhd这样的数据
     * @return
     */
    private boolean rename(String oldname,String newname,String path){
        if(StringUtils.isBlank(oldname) || StringUtils.isBlank(newname) || StringUtils.isBlank(path)){
            return false;
        }

        //读取mhd文件，修改其中的ElementDataFile属性，这个属性是raw文件名。
        String mhdFilePath = path+File.separator+oldname+".mhd";
        String rawFilePath = path+File.separator+oldname+".raw";
        BufferedReader br = null;
        LineNumberReader lnr = null;
        try {
            br = new BufferedReader(new FileReader(mhdFilePath));
            lnr = new LineNumberReader(new FileReader(mhdFilePath));
            lnr.skip(new File(mhdFilePath).length());
            String []mhdtemp = new String[lnr.getLineNumber()];
            String temp = null;
            int number = 0;
            int positionline = -1;
            while((temp = br.readLine()) != null){
                if(temp.startsWith("ElementDataFile"))
                    positionline = number;
                mhdtemp[number++] = temp;
            }

            br.close();
            lnr.close();

            mhdtemp[positionline] = "ElementDataFile = "+ newname+".raw";
            FileWriter fw = new FileWriter(mhdFilePath);
            for(String line : mhdtemp){
                fw.write(line);
                fw.write("\n");
            }
            fw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //修改raw文件名
        new File(rawFilePath).renameTo(new File(path+File.separator+newname+".raw"));
        new File(mhdFilePath).renameTo(new File(path+File.separator+newname+".mhd"));

        return true;
    }

    public static JSONObject parseJSON(String file){
        List<String> list = new LinkedList<String>();
        String text = null;
        try {
            InputStream inputStream = new FileInputStream(file);
            text = org.apache.commons.io.IOUtils.toString(inputStream,"utf8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(text==null){
            return null;
        }
        JSONObject result = new JSONObject();
        JSONObject json = new JSONObject();
        JSONReader reader = new JSONReader(new StringReader(text));
        reader.startObject();
        while (reader.hasNext()) {
            String key = reader.readString();
            if (key.equals("data")) {
                reader.startObject();
                while (reader.hasNext()) {
                    String tag = reader.readString();
                    reader.startArray();
                    JSONArray tagArr = new JSONArray();
                    while (reader.hasNext()) {
                        reader.startObject();
                        JSONObject obj = new JSONObject();
                        while(reader.hasNext()){
                            String k = reader.readString();
                            String v = reader.readString();
                            obj.put(k,v);
                        }
                        tagArr.add(obj);
                        reader.endObject();
                    }
                    result.put(tag,tagArr);
                    reader.endArray();
                }
                reader.endObject();
            }else if(key.equals("code")) {
                reader.readString();
            }else if(key.equals("total")){
                reader.readLong();
            }
        }
        reader.endObject();
        return result;
    }

}
//                            if("organ".equals(k)){
//                                obj.put(k,v);
//                            }else if("InstanceId".equals(k)){
//                                obj.put(k,v);
//                            }else if("hdfspath".equals(k)){
//
//                            }