import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class AirlineMapper extends Mapper<IntWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] words = value.toString().split(",");
        int airportId = Integer.parseInt(words[14]);
        int arrivalDelay = Integer.parseFloat(words[18]);

    }
}