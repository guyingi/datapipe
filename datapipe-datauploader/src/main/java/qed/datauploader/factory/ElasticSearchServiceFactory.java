package qed.datauploader.factory;

import qed.datauploader.service.ElasticSearchService;
import qed.datauploader.service.impl.ElasticSearchServiceImpl;

public class ElasticSearchServiceFactory {
    public static ElasticSearchService getElasticSearchService(){
        return new ElasticSearchServiceImpl();
    }
}
