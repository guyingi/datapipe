package yasen.datauploader.factory;

import yasen.datauploader.service.HdfsService;
import yasen.datauploader.service.impl.HdfsServiceImpl;

public class HdfsServiceFactory {
    public static HdfsService getHdfsService(){
        return new HdfsServiceImpl();
    }
}
