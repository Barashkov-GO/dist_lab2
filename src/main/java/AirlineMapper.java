package ru.barashkov.distributed.airport;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static const int airportIdId = 14;
    private static const int flightDelayId = 18;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(",");
        if (!stringSlices[airportId].equals("\"DEST_AIRPORT_ID\"") && key.get() > 0) {
            int airportId = Integer.parseInt(stringSlices[airportIdId]);
            String flightDelayStr = stringSlices[flightDelayId];
            if (!flightDelayStr.equals("") && Float.parseFloat(flightDelayStr) != 0.0f){
                context.write(new AirportWritableComparable(airportId, 1), new Text(flightDelayStr));
            }
        }
    }
}