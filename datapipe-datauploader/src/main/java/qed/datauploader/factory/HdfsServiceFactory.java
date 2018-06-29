package qed.datauploader.factory;

import qed.datauploader.service.HdfsService;
import qed.datauploader.service.impl.HdfsServiceImpl;

public class HdfsServiceFactory {
    public static HdfsService getHdfsService(){
        return new HdfsServiceImpl();
    }
}
