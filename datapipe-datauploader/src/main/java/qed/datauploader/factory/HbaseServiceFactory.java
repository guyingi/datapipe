package qed.datauploader.factory;

import qed.datauploader.service.HbaseService;
import qed.datauploader.service.impl.HbaseServiceImpl;

public class HbaseServiceFactory {
    public static HbaseService getHbaseService(){
        return new HbaseServiceImpl();
    }
}
