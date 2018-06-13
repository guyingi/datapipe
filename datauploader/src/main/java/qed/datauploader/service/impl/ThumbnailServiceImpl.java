package qed.datauploader.service.impl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import qed.datauploader.config.UploaderConfiguration;
import qed.datauploader.help.StreamGobbler;
import qed.datauploader.service.ThumbnailService;
import qed.datauploader.consts.SysConsts;

import java.io.*;

public class ThumbnailServiceImpl implements ThumbnailService {
    static Logger logger = Logger.getLogger(ThumbnailServiceImpl.class.getName());

    UploaderConfiguration uploadconf;

    public ThumbnailServiceImpl(){
        uploadconf = new UploaderConfiguration();
    }


    @Override
    public byte[] createThumbnail(String source) {
        File file = new File(source);
        String name = file.getName();
        name = name.substring(0,name.indexOf("."));
        String temp = uploadconf.getTempdir();
        String tempfile = temp+File.separator+name+".jpeg";
        String commandStr = uploadconf.getDcm2jpgScriptPath();

        //放置路径有空格
        source = source;
        tempfile  =tempfile;

        try {
            if(exeCmdOnLinux(commandStr,source,tempfile)){
                FileInputStream fin = new FileInputStream(new File(tempfile));
                int available = fin.available();
                byte[] data = new byte[available];
                fin.read(data);
                fin.close();
                new File(tempfile).delete();
                return data;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    //linux上运行版本
    public boolean exeCmdOnLinux(String command,String source,String tempfile) {
        String[] commands = new String[]{command,source,tempfile};
        System.out.println("commandStr:"+command+" "+source+" "+tempfile);
        logger.log(Level.INFO,"commandStr:"+command+" "+source+" "+tempfile);
        BufferedReader br = null;
        try {
            Process proc = Runtime.getRuntime().exec(commands);
            StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "Error");
            StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "Output");
            errorGobbler.start();
            outputGobbler.start();
            proc.waitFor();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    //linux上运行版本
    public boolean exeCmdOnWindows(String command,String source,String tempfile) {
        String commandStr = command+ SysConsts.SPACE+"\'"+source+"\'"+SysConsts.SPACE+"\'"+tempfile+"\'";
        System.out.println("commandStr:"+commandStr);
        BufferedReader br = null;
        try {
            Process proc = Runtime.getRuntime().exec(commandStr);
            StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "Error");
            StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "Output");
            errorGobbler.start();
            outputGobbler.start();
            proc.waitFor();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }


}
