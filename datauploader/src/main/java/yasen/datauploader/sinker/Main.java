package yasen.datauploader.sinker;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import yasen.datauploader.consts.SysConsts;
import yasen.datauploader.tool.DataUploaderTool;
import yasen.datauploader.tool.Statistics;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    static Logger logger = Logger.getLogger(Uploader.class.getName());
    public static void main(String[] args) throws InterruptedException {
//        String dir = "F:\\dicom\\标记测试数据\\dicom\\breast";
//        String dir = "F:\\dicom\\new\\des\\00000c6d-dc77763e-5249ac89-f0aeae65-027a8812\\EY624867 Dong Guo Qin\\ZX1845538 ScreeningBilateral Mammography\\MG R CC";
//        String dir  ="F:\\dicom\\new\\shibai";
//        upload(dir);

        if(args.length!=0 && args[0].length()!=0 && checkParameter(args[0])) {
            logger.log(Level.INFO,"读取dicom路径："+args[0]);
            System.out.println("读取dicom路径："+args[0]);
            upload(args[0]);
        }else{
            logger.log(Level.ERROR,"传入路径错误，可能是分隔符错误或者文件不存在");
            printUsage();
        }
    }

    /*************分配线程池多线程上传****************/
    public static void upload(String dirPath) throws InterruptedException {
        long start = new Date().getTime();
        long sum;

        //传入总目录，返回所有序列目录
        List<String> seriesDirList = DataUploaderTool.listSeriesDir(dirPath);
        sum = seriesDirList.size();
        logger.log(Level.INFO,"扫描到序列："+sum);
        System.out.println("总共目录："+sum);

        Map<Integer,List<String>> map = new HashMap<Integer,List<String>>();
        for(int i = 0; i< SysConsts.THREAD_NUMBER; i++){
            map.put(i,new LinkedList<String>());
        }

        for(int i=0;i<sum;i++){
            List<String> tempList = map.get(i % SysConsts.THREAD_NUMBER);
            tempList.add(seriesDirList.get(i));
        }
        ExecutorService executorService = Executors.newFixedThreadPool(SysConsts.THREAD_NUMBER);

        for(int i = 0; i<SysConsts.THREAD_NUMBER; i++) {
            List<String> subDicomlist = map.get(i);
//            new Thread(new Uploader(subDicomlist)).start();
            executorService.submit(new Uploader(subDicomlist));
        }
        executorService.shutdown();
        try {
            // 当线程池中的所有任务执行完毕时,就会关闭线程池
            while (!executorService.awaitTermination(2, TimeUnit.SECONDS))
                ;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        Thread.sleep(3000);
        Statistics.fail = sum - Statistics.success;
        long end = new Date().getTime();
        logger.log(Level.INFO,"目录总数:"+sum+" 成功:"+ Statistics.success+" 失败数:"+Statistics.fail+" 耗时:"+(end-start)/1000+"s");
        System.out.println("目录总数:"+sum+" 成功:"+ Statistics.success+" 失败数:"+Statistics.fail+" 耗时:"+(end-start)/1000+"s");
    }

    public static boolean checkParameter(String str){
        if(!(new File(str).exists())){
            return false;
        }
        return true;
    }

    public static void printUsage(){
        System.out.println("Usage:java -jar datauploader-1.0.jar sourceFileDirectory\nsourceFileDirectory:存放待上传的dicom文件目录");
    }
}
