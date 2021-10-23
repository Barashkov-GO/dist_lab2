package ru.barashkov.distributed;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final int AIRPORT_ID_ID = 14;
    private static final int FLIGHT_DELAY_ID = 18;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(",");
        if (!stringSlices[AIRPORT_ID_ID].equals("\"DEST_AIRPORT_ID\"") && key.get() > 0) {
            int airportId = Integer.parseInt(stringSlices[AIRPORT_ID_ID]);
            String flightDelayStr = stringSlices[FLIGHT_DELAY_ID];
            if (!flightDelayStr.equals("") && Float.parseFloat(flightDelayStr) != 0.0f){
                context.write(new AirportWritableComparable(airportId, 1), new Text(flightDelayStr));
            }
        }
    }
}