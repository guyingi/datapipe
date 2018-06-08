package yasen.datauploader.sinker;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import yasen.datauploader.config.UploaderConfiguration;
import yasen.datauploader.consts.DataTypeEnum;
import yasen.datauploader.factory.IndexFromHBaseServiceFactory;
import yasen.datauploader.service.IndexFromHBaseService;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class IndexFromHbase {
    public static void main(String[] args){
        IndexFromHBaseService indexFromHBaseService = IndexFromHBaseServiceFactory.getIndexFromHBaseService();
        int result = indexFromHBaseService.indexFromHBaseIncrement(DataTypeEnum.DICOM);
//        int result = indexFromHBaseService.indexFromHBaseFull(DataTypeEnum.DICOM);
    }


//    public void test(){
////        SAXBuilder builder=new SAXBuilder();
////        InputStreamReader reader = new InputStreamReader(UploaderConfiguration.class.getClassLoader().getResourceAsStream("metadatatype.xml"));
////        List<String> DCM_META_DATATYPE_INTEGER = new ArrayList<String>();
////        List<String> DCM_META_DATATYPE_LONG = new ArrayList<String>();
////        List<String> DCM_META_DATATYPE_DOUBLE = new ArrayList<String>();
////        System.out.println("开始解析");
////
////        Document doc=builder.build(reader);
////        Element rootElement = doc.getRootElement();
////        System.out.println(rootElement.getName());
////        List children = rootElement.getChildren();
////        for(Object element : children){
////            String datatype = ((Element)element).getName();
////            if("integer".equals(datatype)){
////                List fields = ((Element) element).getChildren();
////                for(Object field : fields){
////                    String value = ((Element)field).getValue();
////                    DCM_META_DATATYPE_INTEGER.add(value);
////                }
////            }
////            if("long".equals(datatype)){
////                List fields = ((Element) element).getChildren();
////                for(Object field : fields){
////                    String value = ((Element)field).getValue();
////                    DCM_META_DATATYPE_LONG.add(value);
////                }
////            }
////            if("double".equals(datatype)){
////                List fields = ((Element) element).getChildren();
////                for(Object field : fields){
////                    String value = ((Element)field).getValue();
////                    DCM_META_DATATYPE_DOUBLE.add(value);
////                }
////            }
////        }
////
////        for(String e : DCM_META_DATATYPE_INTEGER){
////            System.out.println(e);
////        }
////        for(String e : DCM_META_DATATYPE_LONG){
////            System.out.println(e);
////        }
////        for(String e : DCM_META_DATATYPE_DOUBLE){
////            System.out.println(e);
////        }
////    }

}
