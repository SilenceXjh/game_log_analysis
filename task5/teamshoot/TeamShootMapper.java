package teamshoot;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TeamShootMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(",", -1);
        if(items[5].equals("team025") || items[5].equals("team028")) {
            context.write(new Text(items[5]), new Text(items[0]));
            if(!items[7].isEmpty()) {
                if(items[7].charAt(0) == '2') {
                    if(items[8].equals("make")) {
                        context.write(new Text(items[5]), new Text("2,1"));
                    }
                    else {
                        context.write(new Text(items[5]), new Text("2,0"));
                    }
                }
                else {
                    if(items[8].equals("make")) {
                        context.write(new Text(items[5]), new Text("3,1"));
                    }
                    else {
                        context.write(new Text(items[5]), new Text("3,0"));
                    }
                }
            }
            if(!items[14].isEmpty()) {
                if(items[14].equals("make")) {
                    context.write(new Text(items[5]), new Text("1,1"));
                }
                else {
                    context.write(new Text(items[5]), new Text("1,0"));
                }
            }
        }
    }
}