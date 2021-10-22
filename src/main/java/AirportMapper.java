import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class WordMapper extends Mapper<IntWritable, Text, IntWritable> {