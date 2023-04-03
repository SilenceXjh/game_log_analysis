package teamshoot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class TeamShootCombiner extends Reducer<Text, Text, Text, Text> {

    private HashSet<String> dates = new HashSet<>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values) {
            String item = value.toString();
            if(item.length() == 3) {
                context.write(key, value);
            }
            else {
                dates.add(item);
            }
        }
        String count = "" + dates.size();
        context.write(key, new Text(count));
    }
}