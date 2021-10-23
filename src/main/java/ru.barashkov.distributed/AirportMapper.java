package ru.barashkov.distributed;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final String SEPARATOR = ",";
    private static final String TO_DELETE = "\"";
    private static final String CHECK = "Code";

    private static final int AIRPORT_ID_ID = 14;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().replaceAll(TO_DELETE, "").split(SEPARATOR, 2);
        if (!stringSlices[0].equals(CHECK)) {
            context.write(new AirportWritableComparable(Integer.parseInt(stringSlices[0]), 0), new Text(stringSlices[1]));
        }
    }
}