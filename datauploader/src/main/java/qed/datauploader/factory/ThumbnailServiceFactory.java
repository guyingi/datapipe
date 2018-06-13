package qed.datauploader.factory;

import qed.datauploader.service.ThumbnailService;
import qed.datauploader.service.impl.ThumbnailServiceImpl;

public class ThumbnailServiceFactory {
    public static ThumbnailService getThumbnailService(){
        return new ThumbnailServiceImpl();
    }
}
