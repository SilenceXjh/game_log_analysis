
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class PredictionReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int score = 0;
        int win = 0;
        int all = 0;
        for (Text value : values){
            String fields[] = value.toString().split(",");
            if (fields[0].equals("0")){
                score += Integer.parseInt(fields[1]);
            }
            else {
                score += Integer.parseInt(fields[1]);
                win += 1;
            }
            all += 1;
        }
        double rate = (double)win/all;
        context.write(key, new Text(Double.toString(rate)+"," + Double.toString((double)score/all)));
    }
}
