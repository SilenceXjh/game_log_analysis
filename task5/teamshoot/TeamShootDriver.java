package teamshoot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TeamShootDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length != 2) {
            System.err.println("Usage: jar <in> <out>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "TeamShoot Job");
        job.setJarByClass(TeamShootDriver.class);
        job.setMapperClass(TeamShootMapper.class);
        job.setCombinerClass(TeamShootCombiner.class);
        job.setReducerClass(TeamShootReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}