package task4;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class SPRMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length <= 5) {
            return;
        }
        if (fields.length > 8) {
            if (!fields[6].equals("")) {
                context.write(new Text(fields[6]), new Text(fields[7] + ",1"));
                if (fields[8].equals("make")) {
                    if (fields[7].equals("2-pt")) {
                        context.write(new Text(fields[6]), new Text("score,2"));
                    } else if (fields[7].equals("3-pt")) {
                        context.write(new Text(fields[6]), new Text("score,3"));
                    }
                    if (fields.length > 9 && !fields[9].equals("")) {
                        context.write(new Text(fields[9]), new Text("assist,1"));
                    }
                } else if (fields[8].equals("miss")) {
                    if (fields.length > 10 && !fields[10].equals("")) {
                        context.write(new Text(fields[10]), new Text("block,1"));
                    }
                }
            }
        }
        if (fields.length > 12) {
            if (!fields[11].equals("") && !fields[11].equals("Team")) {
                if (fields[12].equals("defensive")) {
                    context.write(new Text(fields[11]), new Text("d_rebound,1"));
                } else if (fields[12].equals("offensive")) {
                    context.write(new Text(fields[11]), new Text("o_rebound,1"));
                }
            }
        }
        if (fields.length > 14) {
            if (!fields[13].equals("")) {
                context.write(new Text(fields[13]), new Text("1-pt,1"));
                if (fields[14].equals("make")) {
                    context.write(new Text(fields[13]), new Text("score,1"));
                }
            }
        }
        if (fields.length > 18) {
            if (!fields[17].equals("") && !fields[17].equals("Team")) {
                context.write(new Text(fields[17]), new Text("turnover,1"));
            }
            if (fields.length > 19 && !fields[19].equals("")) {
                context.write(new Text(fields[19]), new Text("steal,1"));
            }
        }
    }
}
