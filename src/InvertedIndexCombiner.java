import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(Text val:values)
            sum += Integer.parseInt(val.toString());
        int splitIndex = key.toString().indexOf(":");
        result.set(key.toString().substring(splitIndex+1) + ":" + sum);
        key.set(key.toString().substring(0, splitIndex));
        context.write(key, result);
    }
}
