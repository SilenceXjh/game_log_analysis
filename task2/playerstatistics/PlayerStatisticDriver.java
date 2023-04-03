package playerstatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PlayerStatisticDriver {

    private final static String interResultPath = "temp";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "PlayerData Job");
        job1.setJarByClass(PlayerStatisticDriver.class);
        job1.setMapperClass(PlayerDataMapper.class);
        job1.setReducerClass(PlayerDataReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(interResultPath));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "PlayerSort Job");
        job2.setJarByClass(PlayerStatisticDriver.class);
        job2.setMapperClass(PlayerSortMapper.class);
        job2.setReducerClass(PlayerSortReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(interResultPath));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
    }
}