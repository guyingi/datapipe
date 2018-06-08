package yasen.datauploader.factory;

import yasen.datauploader.service.IndexFromHBaseService;
import yasen.datauploader.service.impl.IndexFromHBaseServiceImpl;

public class IndexFromHBaseServiceFactory {
    public static IndexFromHBaseService getIndexFromHBaseService(){
        return new IndexFromHBaseServiceImpl();
    }
}
