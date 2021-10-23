package ru.barashkov.distributed;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirlineMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final int AIRPORT_ID_POS = 14;
    private static final int FLIGHT_DELAY_POS = 18;
    private static final String SEPARATOR = ",";
    private static final String CHECK_WRONG = "\"DEST_AIRPORT_ID\"";
    private static final int INDICATOR = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().split(SEPARATOR);

        if (!stringSlices[AIRPORT_ID_POS].equals(CHECK_WRONG)) {
            int airportId = Integer.parseInt(stringSlices[AIRPORT_ID_POS]);
            String flightDelayStr = stringSlices[FLIGHT_DELAY_POS];
            if (!flightDelayStr.equals("") && Float.parseFloat(flightDelayStr) != 0.0f){
                context.write(new AirportWritableComparable(airportId, INDICATOR), new Text(flightDelayStr));
            }
        }
    }
}