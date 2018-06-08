package yasen.datauploader.factory;

import yasen.datauploader.service.ThumbnailService;
import yasen.datauploader.service.impl.ThumbnailServiceImpl;

public class ThumbnailServiceFactory {
    public static ThumbnailService getThumbnailService(){
        return new ThumbnailServiceImpl();
    }
}
