package qed.datauploader.service;

import java.awt.*;

public interface ThumbnailService {
    byte[] createThumbnail(String path);

    /**
     * 将dicom转换成jpg图片，以目录为单位进行转换
     * @param dcmPath
     * @param jpgPath
     */
    void convertDcm2Jpeg(String dcmPath,String jpgPath);
}
