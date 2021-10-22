import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(",");
        if (!stringSlices[14].equals("\"DEST_AIRPORT_ID\"") && key.get() > 0) {
            int airportId = Integer.parseInt(stringSlices[14]);
            if (!stringSlices[18].equals("")){
                context.write(new AirportWritableComparable(airportId, 1), new Text(stringSlices[18]));
            }
        }
    }
}