package task4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SPRReducer extends Reducer<Text,Text,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int score = 0, pt1 = 0, pt2 = 0, pt3 = 0;
        int drbd = 0, orbd = 0, assist = 0;
        int steal = 0, block = 0, turnover = 0;
        for (Text value : values) {
            String[] f = value.toString().split(",");
            if (f[0].equals("score")) {
                score += Integer.parseInt(f[1]);
            } else if (f[0].equals("1-pt")) {
                pt1++;
            } else if (f[0].equals("2-pt")) {
                pt2++;
            } else if (f[0].equals("3-pt")) {
                pt3++;
            } else if (f[0].equals("d_rebound")) {
                drbd++;
            } else if (f[0].equals("o_rebound")) {
                orbd++;
            } else if (f[0].equals("assist")) {
                assist++;
            } else if (f[0].equals("steal")) {
                steal++;
            } else if (f[0].equals("block")) {
                block++;
            } else if (f[0].equals("turnover")) {
                turnover++;
            }
        }
        int v = score * 10 - 10 * pt2 - 8 * pt3 - 3 * pt1 + 3 * orbd + 2 * drbd +
                5 * assist + 15 * steal + 7 * block - 12 * turnover;
        context.write(key, new IntWritable(v));
    }
}
