import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class PredictionMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String data[] = value.toString().split(",");
        int homescore = Integer.parseInt(data[2]);
        int awayscore = Integer.parseInt(data[4]);
        if (homescore > awayscore){
            context.write(new Text(data[1]), new Text("1," + Integer.toString(homescore)));
            context.write(new Text(data[3]), new Text("0," + Integer.toString(awayscore)));
        }
        else{
            context.write(new Text(data[1]), new Text("0," + Integer.toString(homescore)));
            context.write(new Text(data[3]), new Text("1," + Integer.toString(awayscore)));
        }
    }
}
