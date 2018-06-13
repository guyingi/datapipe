package qed.datauploader.factory;

import qed.datauploader.service.DesensitizationService;
import qed.datauploader.service.impl.DesensitizationServiceImpl;

public class DesensitizationServiceFactory {
    public static DesensitizationService getDesensitizationService(){
        return new DesensitizationServiceImpl();
    }
}
