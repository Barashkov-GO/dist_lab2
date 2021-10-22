import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;

public class AirportPartitioner extends <AirportWritableComparable, Text> {
    
    @Override
    public abstract int getPartition(AirportWritableComparable flight, Text text, int numPartitions) {
        return (flight.airportId & Integer.MAX_VALUE) % numPartitions;
    }
}
