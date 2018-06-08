package yasen.datauploader.service;

import yasen.datauploader.consts.DataTypeEnum;

public interface IndexFromHBaseService {

    /**
     * 全量索引
     * @return
     */
    int indexFromHBaseFull(DataTypeEnum typeEnum);

    /**
     * dicom的增量索引
     * @return
     */
    int indexFromHBaseIncrement(DataTypeEnum typeEnum);
}
