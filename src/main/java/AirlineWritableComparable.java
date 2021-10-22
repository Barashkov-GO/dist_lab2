import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;

public class AirlineWritable implements WritableComparable {
    private int destinationAirportId;
    private int arrivalDelay;

    AirlineWritable(int destinationAirportId, int arrivalDelay){
        this->destinationAirportId = destinationAirportId;
        this->arrivalDelay = arrivalDelay;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this->destinationAirportId = in.readInt();
        this->arrivalDelay = in.readInt();
    }
}