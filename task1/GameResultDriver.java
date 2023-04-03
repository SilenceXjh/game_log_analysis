package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GameResultDriver {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: jar <in> <out>");
            System.exit(2);
        }
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf,"game_result");
            job.setJarByClass(GameResultDriver.class);

            job.setMapperClass(GameResultMapper.class);
            job.setReducerClass(GameResultReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
