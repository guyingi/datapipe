package qed.datauploader.service;

import java.io.IOException;

public interface DesensitizationService {

    /**给我一个存有SeriesUID的文件路径或者json数组对象，或者list，
     * 和脱敏后的文件的存储路径我就可以对这批数据做脱敏处理
     * @param seriesUIDFile
     * @param destination 存放脱敏文件的路径
     * @return     返回做脱敏成功的个数
     */
     int desensitization(String seriesUIDFile,String destination);

    /**
     *传脱敏数据的方法，传入存放脱敏数据的目录，该目录下面每个序列存放一个目录，
     * 每个序列下面就是该序列的脱敏数据，该序列目录名称为SeriesUID。该目录名承担着重要信息传递的作用
     *  yasen/bigdata/raw /标签/ year/month/day/文件
     *  文件：标签_患者名拼音年龄性别_【Info.csv、LCC.mhd、LCC.raw、LMLO.mhd、LMLO.raw、RCC.mhd、RCC.raw、RMLO.mhd、RMLO.raw、ROI.csv】
     * @param desensitizationDir
     * @param tag     外部传入标记，可能提供一个页面，在页面查询出一批数据，然后将这批数据脱敏，然后输入一个标记，打上去，完美
     * @return  返回上传成功的个数
     */
     int uploadDicomDesensitization(String desensitizationDir,String tag) throws IOException;
}
