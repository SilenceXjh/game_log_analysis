package teamshoot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TeamShootReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int try1 = 0, make1 = 0, try2 = 0, make2 = 0, try3 = 0, make3 = 0;
        for(Text value : values) {
            String item = value.toString();
            if(item.contains(",")) {
                if(item.charAt(0) == '1') {
                    try1++;
                    if(item.charAt(2) == '1') {
                        make1++;
                    }
                }
                else if(item.charAt(0) == '2') {
                    try2++;
                    if(item.charAt(2) == '1') {
                        make2++;
                    }
                }
                else if(item.charAt(0) == '3'){
                    try3++;
                    if(item.charAt(2) == '1') {
                        make3++;
                    }
                }
            }
            else {
                count = Integer.valueOf(item);
            }
        }
        double atry1 = (double)try1 / count;
        double amake1 = (double)make1 / count;
        double atry2 = (double)try2 / count;
        double amake2 = (double)make2 / count;
        double atry3 = (double)try3 / count;
        double amake3 = (double)make3 / count;
        String info1 = "FreeThrow: try:" + String.format("%.1f", atry1) + " make:" + String.format("%.1f", amake1)
                + " percentage:" + String.format("%.2f", amake1 / atry1 * 100) + "%";
        String info2 = "2-pt: try:" + String.format("%.1f", atry2) + " make:" + String.format("%.1f", amake2)
                + " percentage:" + String.format("%.2f", amake2 / atry2 * 100) + "%";
        String info3 = "3-pt: try:" + String.format("%.1f", atry3) + " make:" + String.format("%.1f", amake3)
                + " percentage:" + String.format("%.2f", amake3 / atry3 * 100) + "%";
        context.write(key, new Text(info3));
        context.write(key, new Text(info2));
        context.write(key, new Text(info1));
    }
}