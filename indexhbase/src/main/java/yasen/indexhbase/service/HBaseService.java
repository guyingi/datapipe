package yasen.indexhbase.service;

import java.io.IOException;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.indexhbase.service
 * @Description: ${todo}
 * @date 2018/6/6 16:07
 */
public interface HBaseService {
    void put(String tablename, String rowkey,String cf, String qualify,String value) throws IOException;

    void delete(String tablename, String rowkey) throws IOException;
}
