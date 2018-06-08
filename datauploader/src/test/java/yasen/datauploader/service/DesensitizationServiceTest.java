package yasen.datauploader.service;

import yasen.datauploader.factory.DesensitizationServiceFactory;

import java.io.IOException;

public class DesensitizationServiceTest {

    public static void main(String[] args) throws IOException {
        String path = "F:\\lab\\disensitization";
        DesensitizationService desensitizationService = DesensitizationServiceFactory.getDesensitizationService();
        int success = desensitizationService.uploadDicomDesensitization(path,"协和脑核");

    }
}
