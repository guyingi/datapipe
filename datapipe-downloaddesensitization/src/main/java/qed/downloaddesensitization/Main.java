package qed.downloaddesensitization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mortbay.util.StringUtil;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package qed.downloaddesensitization
 * @Description: ${todo}
 * @date 2018/6/25 19:16
 */
public class Main {

    public static Logger logger = Logger.getLogger(Main.class);
    public static void main(String[] args) {
        JSONArray argsJson = JSON.parseArray(JSON.toJSONString(args));
        logger.log(Level.INFO,argsJson.toJSONString());

        String jsonFilePath  = null;
        String desPath = null;

        if(args.length==2) {
            jsonFilePath = args[0];
            desPath = args[1];
        }
        if(StringUtils.isBlank(jsonFilePath) || StringUtils.isBlank(desPath)){
            usage();
        }
        new Download().doDownload(jsonFilePath,desPath);
    }

    public static void usage(){
        System.out.println("usage: downloaddesentisize.jar jsonFilePath des");
    }
}
