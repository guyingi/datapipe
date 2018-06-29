import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author WeiGuangWu
 * @version V1.0
 * @Package PACKAGE_NAME
 * @Description: ${todo}
 * @date 2018/6/27 17:47
 */
public class Test {
    public static void main(String[] args) throws IOException {
        BufferedReader big = new BufferedReader(new FileReader(new File("F:\\实验室\\数据库合并\\322.txt")));
        BufferedReader small = new BufferedReader(new FileReader(new File("F:\\实验室\\数据库合并\\428.txt")));
        List<String> bigList = new ArrayList<String>();
        List<String> smallList = new ArrayList<String>();

        String line;
        while((line=big.readLine())!=null){
            bigList.add(line);
        }
        big.close();

        while((line=small.readLine())!=null){
            smallList.add(line);
        }
        small.close();

        List<String> common = new ArrayList<String>();
        for(String str : smallList){
            if(bigList.contains(str)){
                common.add(str);
            }
        }
        for(String str : common){
            System.out.println(str);
        }
    }
}
