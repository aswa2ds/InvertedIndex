import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 这里是我妄图搞一搞全局排序，但是没跑出来 嘤嘤嘤
public class InvertedIndexSort extends WritableComparator {
    public InvertedIndexSort() {
        super(Text.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b){
        if (a == null || b == null)
            return 0;
        System.out.println(a.toString() + " " + b.toString());

        double aAvg = Double.parseDouble(a.toString().split("\t")[1]);
        double bAvg = Double.parseDouble(b.toString().split("\t")[1]);
        return (int)(bAvg - aAvg);
    }
}
