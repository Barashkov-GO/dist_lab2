package ru.barashkov.distributed;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlightsApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3){
            System.err.println("Count of arguments doesn't match to \n\tApp [airlines] [airports] [output]");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(FlightsApp.class);
        job.setJobName("Airports reduce side join");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AirlineMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(AirportPartitioner.class);
        job.setGroupingComparatorClass(AirportComparator.class);
        job.setReducerClass(AirportReducer.class);
        job.setMapOutputKeyClass(AirportWritableComparable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}