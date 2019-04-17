import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileList = new String();
        double sum = 0, count = 0;
        for(Text val: values){
            count++;
            fileList += val.toString() + ";";
            int splitIndex = val.toString().indexOf(":");
            sum += Integer.parseInt(val.toString().substring(splitIndex+1));
        }
        double avg = sum/count;
        result.set(fileList);
        key.set(key.toString() + "\t" + String.format("%.2f", avg) + ",");
        context.write(key, result);
    }
}
