package yasen.datauploader.factory;

import yasen.datauploader.service.HbaseService;
import yasen.datauploader.service.impl.HbaseServiceImpl;

public class HbaseServiceFactory {
    public static HbaseService getHbaseService(){
        return new HbaseServiceImpl();
    }
}
