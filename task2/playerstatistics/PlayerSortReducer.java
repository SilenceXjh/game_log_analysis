package playerstatistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PlayerSortReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<PlayerInfo> players = new ArrayList<>();
        for(Text value : values) {
            players.add(new PlayerInfo(value.toString()));
        }
        Collections.sort(players);
        String res = "";
        for(int i = 0; i < 5; ++i) {
            res = res + (i+1) + "." + players.get(i).getName() +
                    ":" + players.get(i).getValue() + " ";
        }
        context.write(key, new Text(res));
    }
}

class PlayerInfo implements Comparable<PlayerInfo> {
    private String name;
    private int value;

    public PlayerInfo(String s) {
        String[] items = s.split(",");
        this.name = items[0];
        this.value = Integer.valueOf(items[1]);
    }

    public String getName() {
        return this.name;
    }

    public int getValue() {
        return this.value;
    }

    @Override
    public int compareTo(PlayerInfo p) {
        return p.value - this.value;
    }
}