
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    Text word = new Text();
    Text one = new Text("1");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 获得文件名（带后缀）
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fullName = fileSplit.getPath().getName();
        // 通过字符串分割去掉后缀，应该可以改成:
        // String fileName = fullName.split(".")[0];
        int splitIndex = fullName.indexOf(".");
        String fileName = fullName.substring(0, splitIndex);
        // 存储成<key, value>， 数据类型为<Text, Text>， 例：<词：书名， “1”>
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken() + ":" + fileName);
            context.write(word, one);
        }
    }
}
