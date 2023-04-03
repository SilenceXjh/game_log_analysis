package task4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text,Text, IntWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        TreeMap<Integer, ArrayList<String>> mp = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        });
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            int spr = Integer.parseInt(fields[1]);
            if (mp.get(spr) == null) {
                ArrayList<String> empty = new ArrayList<>();
                mp.put(spr, empty);
            }
            mp.get(spr).add(fields[0]);
        }
        int i = 0;
        for (ArrayList<String> names : mp.values()) {
            for (String name : names) {
                i++;
                context.write(new IntWritable(i), new Text(name));
                if (i == 5) {
                    break;
                }
            }
            if (i == 5) {
                break;
            }
        }
    }
}
