package yasen.datauploader.test;

import com.alibaba.fastjson.JSONObject;
import yasen.datauploader.tool.DataUploaderTool;
import yasen.datauploader.tool.IMAParseTool;

import java.io.*;
import java.util.Date;
import java.util.List;

public class IMAParseToolTest {
    public static void main(String[] args) throws IOException {
        String paths[] = {"F:\\dicom\\PA025\\ST0\\SE0\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE1\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE2\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE3\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE4\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE5\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE6\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE7\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE8\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE9\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE10\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE11\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE12\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE13\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE14\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE15\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE16\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE17\\IM0",
                "F:\\dicom\\PA025\\ST0\\SE18\\IM0",
        "F:\\dicom\\PETMR\\2\\PET\\IM1.IMA"};
        String str = "F:\\dicom\\PETMR\\Dicom\\LI_SHUQIANG.NM.BRAIN_SCAN.1000.0001.2016.08.18.14.53.08.468750.2034113.IMA";



        long start = new Date().getTime();
        long count = 0;
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("F:\\实验室\\meta.txt")));
        String dirPath = "F:\\dicom";
        List<String> seriesDirList = DataUploaderTool.listSeriesDir(dirPath);
        for(String e : seriesDirList){
            File file = new File(e);
            for(File f : file.listFiles()){
                JSONObject field = IMAParseTool.extractMetaMsg(f.getAbsolutePath());
                if(!field.isEmpty()){
                    bw.write(field.toJSONString());
                    bw.newLine();
                    count++;
                    if(count%100==0)
                        System.out.println(count);
                    break;
                }
            }
        }
        bw.flush();
        bw.close();
        long end = new Date().getTime();
        System.out.println("解析数量："+count+"\t 耗时："+(end-start)/1000+"s\t平均"+(end-start)/1000/count+"s");
//        JSONObject jsonObject = IMAParseTool.extractMetaMsg(str);
//        System.out.println(jsonObject.toJSONString());
//            System.out.println(jsonObject.get(SysConsts.ImageNumber));
//        String dir = "F:\\实验室\\PA000\\016_SilentMR_MRA";
//        int existBadFileOrLost = new Uploader().isExistBadFileOrLost(new File(dir));
//        System.out.println(existBadFileOrLost);
//        File file = new File(dir);
//        for(File e : file.listFiles()){
//            JSONObject jsonObject = IMAParseTool.extractMetaMsg(e.getAbsolutePath());
//            System.out.println(jsonObject.get(SysConsts.ImageNumber));
//        }
//        System.out.println(fileName);
//        System.out.println(fileName.substring(fileName.indexOf(".")+1,fileName.length()));
    }

}
