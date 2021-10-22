import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirportMapper extends Mapper<IntWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().replaceAll("\"", "").split(",");
        if (stringSlices[0] != "Code") {
            context.write(new AirportWritableComparable(Integer.parseInt(stringSlices[0]), 0), new Text(stringSlices[1]));
        }
    }
}