import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 输入格式 <key, <values>>, 数据类型：<Text, <Texts>>, 例：<词, <书名：出现次数, 书名：出现次数,..., 书名：出现次数>>
        String fileList = new String();
        // sum用来累和该key在所有书中出现的次数累和，用于计算平均值；
        // count用来计算该key在多少本书中出现过；
        // 平均值avg=sum/count
        double sum = 0, count = 0;
        for(Text val: values){
            count++;
            fileList += val.toString() + ";";
            // 从value中提取key在每本书中的出现次数
            int splitIndex = val.toString().indexOf(":");
            sum += Integer.parseInt(val.toString().substring(splitIndex+1));
        }
        double avg = sum/count;
        // 改变<key, value>的格式, 数据类型为<Text, Text>, 例：<词   a.bc, 书名：出现次数;书名：出现次数;...书名：出现次数;>
        result.set(fileList);
        key.set(key.toString() + "\t" + String.format("%.2f", avg) + ",");
        context.write(key, result);
    }
}
