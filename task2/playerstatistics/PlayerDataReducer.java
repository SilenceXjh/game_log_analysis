package playerstatistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PlayerDataReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int score = 0, rebound = 0, assist = 0, steal = 0, block = 0;
        for(Text value : values) {
            String[] items = value.toString().split(",");
            if(items[0].equals("score")) {
                score += Integer.valueOf(items[1]);
            }
            else if(items[0].equals("rebound")) {
                rebound++;
            }
            else if(items[0].equals("assist")) {
                assist++;
            }
            else if(items[0].equals("steal")) {
                steal++;
            }
            else if(items[0].equals("block")) {
                block++;
            }
        }
        String info = "" + score + "," + rebound + "," + assist +
                "," + steal + "," + block;
        context.write(key, new Text(info));
    }
}