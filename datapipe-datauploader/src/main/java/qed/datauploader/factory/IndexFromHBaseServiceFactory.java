package qed.datauploader.factory;

import qed.datauploader.service.IndexFromHBaseService;
import qed.datauploader.service.impl.IndexFromHBaseServiceImpl;

public class IndexFromHBaseServiceFactory {
    public static IndexFromHBaseService getIndexFromHBaseService(){
        return new IndexFromHBaseServiceImpl();
    }
}
