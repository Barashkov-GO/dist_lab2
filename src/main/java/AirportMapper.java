import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class WordMapper extends Mapper<IntWritable, Text, AirportWritable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] words = value.toString().ReplaceAll("\"").split(",");
        if (words[0] != "Code") {
            context.write(new AirportWritable(Integerwords[0]), new Text(words[1]));
        }
    }
}