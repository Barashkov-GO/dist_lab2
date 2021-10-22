import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<IntWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(",");
        int airportId = Integer.parseInt(stringSlices[14]);
        int arrivalDelay = Float.parseFloat(stringSlices[18] != "" ? stringSlices[18] : 0);
        if (arrivalDelay != 0.0f) {
            context.write(new AirportWritableComparable(airportId, 1), new Text(String.valueOf(arrivalDelay)));
        }
    }
}