package task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestFiveDriver {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: jar <in> <out>");
            System.exit(2);
        }
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "SPR");
            job.setJarByClass(BestFiveDriver.class);

            job.setMapperClass(SPRMapper.class);
            job.setReducerClass(SPRReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path("temp4"));

            job.waitForCompletion(true);

            Job job2 = new Job(conf, "best_five");
            job2.setJarByClass(BestFiveDriver.class);

            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path("temp4/part-r-00000"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);

            /*Path path = new Path("temp4");
            FileSystem fileSystem = path.getFileSystem(conf);
            fileSystem.delete(path, true);*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
