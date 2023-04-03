package task1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GameResultReducer extends Reducer<Text,Text, NullWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int homescore = 0, awayscore = 0;
        String[] f = key.toString().split(",");
        String date = f[0];
        String home = f[1];
        String away = f[2];
        for (Text value : values) {
            String[] p = value.toString().split(",");
            if (p[0].equals("1")) {
                homescore += Integer.parseInt(p[1]);
            } else {
                awayscore += Integer.parseInt(p[1]);
            }
        }
        Text v = new Text(date + "," + home + "," + homescore + "," + away + "," + awayscore);
        context.write(null, v);
    }
}
