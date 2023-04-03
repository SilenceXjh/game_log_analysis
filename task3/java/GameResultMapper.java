

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class GameResultMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        String k = fields[0] + "," + fields[4] + "," + fields[3];
        if (fields.length <= 5) {
            return;
        }
        if (fields[5].equals(fields[4])) {
            if (fields.length > 8 && fields[7].equals("2-pt") && fields[8].equals("make")) {
                context.write(new Text(k), new Text("1,2"));
            } else if (fields.length > 8 && fields[7].equals("3-pt") && fields[8].equals("make")) {
                context.write(new Text(k), new Text("1,3"));
            } else if (fields.length > 14 && fields[14].equals("make")) {
                context.write(new Text(k), new Text("1,1"));
            }
        } else if (fields[5].equals(fields[3])) {
            if (fields.length > 8 && fields[7].equals("2-pt") && fields[8].equals("make")) {
                context.write(new Text(k), new Text("2,2"));
            } else if (fields.length > 8 && fields[7].equals("3-pt") && fields[8].equals("make")) {
                context.write(new Text(k), new Text("2,3"));
            } else if (fields.length > 14 && fields[14].equals("make")) {
                context.write(new Text(k), new Text("2,1"));
            }
        }
    }
}
