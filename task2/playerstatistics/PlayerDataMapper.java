package playerstatistics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PlayerDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(",", -1);
        if(!items[6].isEmpty()) {
            if(items[8].equals("make")) {
                if (items[7].charAt(0) == '2') {
                    context.write(new Text(items[6]), new Text("score,2"));
                } else {
                    context.write(new Text(items[6]), new Text("score,3"));
                }
            }
        }
        if(!items[9].isEmpty()) {
            context.write(new Text(items[9]), new Text("assist,1"));
        }
        if(!items[10].isEmpty()) {
            context.write(new Text(items[10]), new Text("block,1"));
        }
        if(!items[11].isEmpty()) {
            if(!items[11].equals("Team")) {
                context.write(new Text(items[11]), new Text("rebound,1"));
            }
        }
        if(!items[13].isEmpty()) {
            if(items[14].equals("make")) {
                context.write(new Text(items[13]), new Text("score,1"));
            }
        }
        if(!items[19].isEmpty()) {
            context.write(new Text(items[19]), new Text("steal,1"));
        }
    }
}