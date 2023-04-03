package playerstatistics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PlayerSortMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String name = line.split("\t", 2)[0];
        String info = line.split("\t", 2)[1];
        String[] items = info.split(",");
        context.write(new Text("score"), new Text(name + "," + items[0]));
        context.write(new Text("rebound"), new Text(name + "," + items[1]));
        context.write(new Text("assist"), new Text(name + "," + items[2]));
        context.write(new Text("steal"), new Text(name + "," + items[3]));
        context.write(new Text("block"), new Text(name + "," + items[4]));
    }
}