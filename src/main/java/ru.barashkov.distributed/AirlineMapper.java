package ru.barashkov.distributed;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final int AIRPORT_ID_POSITION = 14;
    private static final int FLIGHT_DELAY_POSITION = 18;
    private static final String SEPARATOR = ",";
    private static final String HEADER = "\"DEST_AIRPORT_ID\"";
    private static final int INDICATOR = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(SEPARATOR);

        if (!stringSlices[AIRPORT_ID_POSITION].equals(HEADER)) {
            int airportId = Integer.parseInt(stringSlices[AIRPORT_ID_POSITION]);
            String flightDelayStr = stringSlices[FLIGHT_DELAY_POSITION];
            if (!flightDelayStr.isEmpty() && Float.parseFloat(flightDelayStr) != 0.0f){
                context.write(new AirportWritableComparable(airportId, INDICATOR), new Text(flightDelayStr));
            }
        }
    }
}