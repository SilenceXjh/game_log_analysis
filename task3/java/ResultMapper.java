import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ResultMapper extends Mapper <LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields[0].equals("AwayTeam") || fields[0].equals("HomeTeam")){

        }
        else {
            context.write(new Text(fields[0]+","+fields[1]), new Text("1"));
        }
    }
}
