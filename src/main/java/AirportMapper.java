package ru.barashkov.distributed;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] stringSlices = value.toString().replaceAll("\"", "").split(",", 2);
        if (!stringSlices[0].equals("Code")) {
            context.write(new AirportWritableComparable(Integer.parseInt(stringSlices[0]), 0), new Text(stringSlices[1]));
        }
    }
}