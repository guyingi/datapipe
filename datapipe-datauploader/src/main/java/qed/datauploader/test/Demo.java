package qed.datauploader.test;

import org.apache.log4j.Logger;
import qed.datauploader.uploader.Uploader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
        BufferedReader bin = new BufferedReader(new FileReader(new File("F:\\实验室\\short.txt")));
        BufferedReader bin2 = new BufferedReader(new FileReader(new File("F:\\实验室\\long.txt")));
        List<String> list = new ArrayList<>();

        String line;
        while((line=bin2.readLine())!=null){
            list.add(line);
        }
        bin2.close();
        System.out.println(list.size());

        while((line=bin.readLine())!=null){
            int index = list.indexOf(line);
            if(index>0){
                list.remove(index);
            }
        }
        bin.close();

        for(String str : list) {
            System.out.println(str);
        }

    }


}
