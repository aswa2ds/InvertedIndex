
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    Text word = new Text();
    Text one = new Text("1");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fullName = fileSplit.getPath().getName();
        int splitIndex = fullName.indexOf(".");
        String fileName = fullName.substring(0, splitIndex);
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken() + ":" + fileName);
            context.write(word, one);
        }
    }
}
