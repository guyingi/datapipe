package yasen.datauploader.factory;

import yasen.datauploader.service.DesensitizationService;
import yasen.datauploader.service.impl.DesensitizationServiceImpl;

public class DesensitizationServiceFactory {
    public static DesensitizationService getDesensitizationService(){
        return new DesensitizationServiceImpl();
    }
}
