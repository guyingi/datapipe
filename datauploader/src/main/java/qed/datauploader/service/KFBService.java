package qed.datauploader.service;

import java.io.IOException;

public interface KFBService {

    int uploadKFB(String kfbDir,String institution) throws IOException;
}
