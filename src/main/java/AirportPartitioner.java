import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class AirportPartitioner extends Partitioner<AirportWritableComparable, Text> {
    @Override
    public int getPartition(AirportWritableComparable flight, Text text, int numPartitions) {
        return (flight.airportId & Integer.MAX_VALUE) % numPartitions;
    }
}
