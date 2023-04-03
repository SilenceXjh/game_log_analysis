import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import  org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PredictionDriver {
    private final static String interResultPath = "task3_temp/temp1";
    private final static String cachepath = "task3_temp/rate.data";

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: jar <in> <in> <out>");
            System.exit(3);
        }
        try {
            Configuration conf1 = new Configuration();
            Job job1 = new Job(conf1, "Gameresult Job");
            job1.setJarByClass(PredictionDriver.class);
            job1.setMapperClass(GameResultMapper.class);
            job1.setReducerClass(GameResultReducer.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1,new Path(interResultPath));
            job1.waitForCompletion(true);

            Configuration conf2 = new Configuration();
            Job job2 = new Job(conf1, "Prediction Job");
            job2.setJarByClass(PredictionDriver.class);
            job2.setMapperClass(PredictionMapper.class);
            job2.setReducerClass(PredictionReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(interResultPath));
            FileOutputFormat.setOutputPath(job2,new Path(cachepath));
            job2.waitForCompletion(true);

            Configuration conf3 = new Configuration();
            Job job3 = new Job(conf3, "Prediction Job");
            job3.setJarByClass(PredictionDriver.class);
            job3.setMapperClass(ResultMapper.class);
            job3.setReducerClass(ResultReducer.class);
            job3.addCacheFile(new Path(cachepath + "/part-r-00000").toUri());

            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job3, new Path(args[1]));
            FileOutputFormat.setOutputPath(job3,new Path(args[2]));
            job3.waitForCompletion(true);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
