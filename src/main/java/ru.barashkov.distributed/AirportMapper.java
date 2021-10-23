package ru.barashkov.distributed;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final String SEPARATOR = ",";
    private static final String TO_DELETE = "\"";
    private static final String CHECK = "Code";
    private static final int SPLIT_MAX = 2;
    private static final int AIRPORT_ID_ID = 0;
    private static final int AIRPORT_NAME_ID = 1;
    private static final int INDICATOR = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().replaceAll(TO_DELETE, "").split(SEPARATOR, SPLIT_MAX);
        if (!stringSlices[0].equals(CHECK)) {
            context.write(new AirportWritableComparable(Integer.parseInt(stringSlices[AIRPORT_ID_ID]), INDICATOR), new Text(stringSlices[AIRPORT_NAME_ID]));
        }
    }
}