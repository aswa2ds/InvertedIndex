import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 输入格式 <key, <values>>, 数据类型：<Text, <Texts>>, 例：<词：书名, <"1", "1", ... , "1">>
        int sum = 0;
        for(Text val:values)
            sum += Integer.parseInt(val.toString()); // 将value转换为int值进行相加， sum为一个词在一本书中出现的次数

        // 改变<key, value>的格式, 数据类型为<Text, Text>, 例：<词, 书名：出现次数>
        int splitIndex = key.toString().indexOf(":");
        result.set(key.toString().substring(splitIndex+1) + ":" + sum);
        key.set(key.toString().substring(0, splitIndex));
        context.write(key, result);
    }
}
