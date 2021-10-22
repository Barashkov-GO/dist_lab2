import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(",");
        if (!stringSlices[14].equals("\"DEST_AIRPORT_ID\"")) {
            int airportId = Integer.parseInt(stringSlices[14]);
            float arrivalDelay = Float.parseFloat(stringSlices[18] != "" ? stringSlices[18] : "0");
            if (arrivalDelay != 0.0f) {
                context.write(new AirportWritableComparable(airportId, 1), new Text(String.valueOf(arrivalDelay)));
            }
        }
    }
}