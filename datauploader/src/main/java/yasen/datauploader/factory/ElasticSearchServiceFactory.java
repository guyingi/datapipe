package yasen.datauploader.factory;

import yasen.datauploader.service.ElasticSearchService;
import yasen.datauploader.service.impl.ElasticSearchServiceImpl;

public class ElasticSearchServiceFactory {
    public static ElasticSearchService getElasticSearchService(){
        return new ElasticSearchServiceImpl();
    }
}
