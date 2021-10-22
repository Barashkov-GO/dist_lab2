import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class WordMapper extends Mapper<IntWritable, Text, AirportWritable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] words = value.toString().replaceAll("[\\n\\r\\t.,!?*\"'\\[\\]{}~$#+=/()]", "")
                .replaceAll("  ", " ")
                .toLowerCase()
                .split(" ");
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(new Text(String.valueOf(word.length())), new IntWritable(1));
            }
        }
    }
}