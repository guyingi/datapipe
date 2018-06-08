package yasen.datauploader.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import yasen.datauploader.config.UploaderConfiguration;
import yasen.datauploader.sinker.Uploader;
import yasen.datauploader.tool.DataUploaderTool;
import yasen.datauploader.tool.IMAParseTool;

import java.io.*;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Pattern;

public class Demo {
    static Logger logger = Logger.getLogger(Uploader.class.getName());
    public static void main(String[] args) throws IOException {
//        BufferedReader br = new BufferedReader(new FileReader(new File("F:\\工作文档\\资料\\DICOM File Meta Elements.csv")));
//        BufferedWriter wr = new BufferedWriter(new FileWriter(new File("F:\\工作文档\\资料\\DicomMetaElements.properties")));
//        String temp;
//        int count = 0;
//        while((temp=br.readLine())!=null){
//            count ++;
////            System.out.println();
//            String[] arr = temp.split(",");
//            String tag = null;
//            try{
//                tag =arr[0]+","+arr[1];
//            }catch (ArrayIndexOutOfBoundsException e){
//                continue;
//            }
////            try{
//            tag = tag.substring(2,11);
////            }catch (StringIndexOutOfBoundsException e){
////                System.out.println(tag);
////            }
//            String keyword = arr[3];
//            keyword = keyword.replace("?","");
//            System.out.println(tag+"\t"+keyword);
//            wr.write(tag+" = "+keyword);
//            wr.newLine();
//        }
//        br.close();
//        wr.close();
//
//        System.out.println(count);


//        Pattern pattern = Pattern.compile("^([\\d|a-zA-Z]{4},[\\d|a-zA-Z]{4})$");
//        System.out.println(DataUploaderTool.isTag("0024,0306"));

//        int n = 6;
//        StringBuilder sb = new StringBuilder();
//        for(int i=0;i<n;i++){
//            sb.append(chars[i]);
//        }

    }


}
