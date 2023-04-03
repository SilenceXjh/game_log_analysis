import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class ResultReducer extends Reducer<Text, Text, Text, Text> {

    private Hashtable<String, List<Double>> rates = new Hashtable<>();
    private double ave_rate = 0.0;
    private double ave_scores = 0.0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();
        if(cacheFiles != null && cacheFiles.length > 0) {
            String line;
            String[] fields;
            BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
            try {
                double sum_rate=0.0,sum_scores=0.0;
                int num=0;
                while((line = reader.readLine()) != null){
                    fields = line.split("\t", 2);
                    String[] vals = fields[1].split(",");
                    List<Double> values = new ArrayList<>();
                    for(String val : vals) {
                        values.add(Double.parseDouble(val));
                    }
                    rates.put(fields[0], values);
                    sum_rate += Double.parseDouble(vals[0]);
                    sum_scores += Double.parseDouble(vals[1]);
                    num++;
                }
                ave_rate = sum_rate/num;
                ave_scores = sum_scores/num;
            }
            finally {
                reader.close();
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String teamnames = key.toString();
        String team1 = teamnames.split(",")[0];
        String team2 = teamnames.split(",")[1];
        Double rate1 = 0.5, rate2 = 0.5, scores1 = 0.0, scores2 = 0.0;
        int exist1 = 0, exist2 = 0;
        for (Map.Entry<String, List<Double>> entry : rates.entrySet()) {
            if (entry.getKey().equals(team1)) {
                rate1 = entry.getValue().get(0);
                scores1 = entry.getValue().get(1);
                exist1 = 1;
            }

            if (entry.getKey().equals(team2)) {
                rate2 = entry.getValue().get(0);
                scores2 = entry.getValue().get(1);
                exist2 = 1;
            }
        }
        Double final_rate1 = 0.5;
        Double final_rate2 = 0.5;
        if (exist1 == 0) {
            rate1 = ave_rate;
            scores1 = ave_scores;
        }
        if (exist2 == 0) {
            rate2 = ave_rate;
            scores2 = ave_scores;
        }
        final_rate1 = 0.9 * rate1 / (rate1 + rate2) + 0.1 * scores1 / (scores1 + scores2);
        final_rate2 = 0.9 * rate2 / (rate1 + rate2) + 0.1 * scores2 / (scores1 + scores2);
        context.write(null, new Text(team1 +","+ Long.toString(Math.round(final_rate1*100)) +"%\t"+team2+","+ Long.toString(Math.round(final_rate2*100)) + "%"));
    }
}
