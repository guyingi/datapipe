package qed.toground.download;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import qed.toground.conf.SysConstants;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    static Logger logger = Logger.getLogger(Main.class);
    public static void main(String[] args) {
//        String confFilePath = "F:\\实验室\\471892933732.json";
//        String desDir  = "F:\\实验室\\infosupplyer";
//        doDownload(confFilePath,desDir);
        String confFilePath = null;
        String desDir = null;
        if(args!=null && args.length==2){
            confFilePath = args[0];
            desDir = args[1];
        }else if(args.length==1 && args[0].equals("-u")){
            printUsage();
        }

        if(confFilePath!=null && confFilePath.length()!=0 && desDir!=null && desDir.length()!=0 && checkParameter(args))
            doDownload(confFilePath,desDir);
        else
            printUsage();

    }

    private static void doDownload(String confFilePath,String desDir) {
        long startTime = new Date().getTime();

        Downloader downloader = new Downloader();
        List<String> paths = downloader.parseJSON(confFilePath);
        int sum = paths.size();
        System.out.println("总共："+sum);
        ExecutorService executorService = Executors.newFixedThreadPool(SysConstants.THREAD_COUNT);
        Map<Integer,List<String>> map = new HashMap<Integer,List<String>>();
        for(int i=0;i<SysConstants.THREAD_COUNT;i++){
            map.put(i,new LinkedList<String>());
        }

        List<String> strings  =null;
        for(int i=0;i<sum;i++){
            strings = map.get(i % SysConstants.THREAD_COUNT);
            strings.add(paths.get(i));
        }

        for(int i=0;i<SysConstants.THREAD_COUNT;i++){
            List<String> strings1 = map.get(i);
            executorService.submit(new MutilThread(strings1,desDir));
        }

        executorService.shutdown();
        while(true){
            if(executorService.isTerminated()){
                System.out.println("所有的子线程都结束了！");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long endTime = new Date().getTime();
        logger.log(Level.INFO,"总共下载 "+sum+" 个序列，耗时 "+(endTime-startTime)/1000+"s");
        System.out.println("总共下载 "+sum+" 个序列，耗时 "+(endTime-startTime)/1000+"s");
}

    public static boolean checkParameter(String[] args){
        Downloader downloader = new Downloader();
        String conf = args[0];
        String desDir = args[1];
        if(downloader.parseJSON(conf)==null){
            logger.error("json文件格式错误");
            return false;
        }
        if(!(new File(desDir).exists())){
            logger.error("指定文件存放目录不存在");
            return false;
        }
        return true;
    }

    public static void printUsage(){
        System.out.println("Usage :   java -jar toground.jar conf des\n\tconf:json下载文件\n\tdes:下载的dicom文件存放目录，需要提前创建" );
    }
}
