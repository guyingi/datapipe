package yasen.datauploader.config;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package yasen.datauploader.config
 * @Description: ${todo}
 * @date 2018/6/6 14:40
 */
public class IndexFromHBaseConfig {

    /**********dicom元数据字段数据类型表*******************/
    private List<String> integerDcmMetaType = new ArrayList<String>();
    private List<String> longDcmMetaType = new ArrayList<String>();
    private List<String> doubleDcmMetaType = new ArrayList<String>();

    /**********dicom的rowkey临时表，用于index from hbase*********************/
    private String dcmRowkeyTablename = null;
    private String dcmRowkeyTableCf = null;
    private String dcmRowkeyTableQualify = null;

    public IndexFromHBaseConfig(){
        init();
    }

    private void init() {
        SAXBuilder builder=new SAXBuilder();
        InputStreamReader reader = new InputStreamReader(UploaderConfiguration.class.getClassLoader().getResourceAsStream("metadatatype.xml"));
        System.out.println("开始解析");

        Document doc= null;
        try {
            doc = builder.build(reader);
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Element rootElement = doc.getRootElement();
        List children = rootElement.getChildren();
        for(Object element : children){
            String datatype = ((Element)element).getName();
            if("integer".equals(datatype)){
                List fields = ((Element) element).getChildren();
                for(Object field : fields){
                    String value = ((Element)field).getValue();
                    integerDcmMetaType.add(value);
                }
            }
            if("long".equals(datatype)){
                List fields = ((Element) element).getChildren();
                for(Object field : fields){
                    String value = ((Element)field).getValue();
                    longDcmMetaType.add(value);
                }
            }
            if("double".equals(datatype)){
                List fields = ((Element) element).getChildren();
                for(Object field : fields){
                    String value = ((Element)field).getValue();
                    doubleDcmMetaType.add(value);
                }
            }
        }

        InputStreamReader reader2 = new InputStreamReader(UploaderConfiguration.class.getClassLoader().getResourceAsStream("datauploader.properties"));
        Properties props = new Properties();
        try {
            props.load(reader2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dcmRowkeyTablename = props.getProperty("dicom.rowkeytemp.tablename");
        dcmRowkeyTableCf = props.getProperty("dicom.rowkeytemp.cf");
        dcmRowkeyTableQualify = props.getProperty("dicom.rowkeytemp.qualify");
    }

    public List<String> getIntegerDcmMetaType() {
        return integerDcmMetaType;
    }

    public List<String> getLongDcmMetaType() {
        return longDcmMetaType;
    }

    public List<String> getDoubleDcmMetaType() {
        return doubleDcmMetaType;
    }

    public String getDcmRowkeyTablename() {
        return dcmRowkeyTablename;
    }

    public String getDcmRowkeyTableCf() {
        return dcmRowkeyTableCf;
    }

    public String getDcmRowkeyTableQualify() {
        return dcmRowkeyTableQualify;
    }
}
