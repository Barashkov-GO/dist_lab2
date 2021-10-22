import org.apache.hadoop.io.Writable;
import java.io.IOException;

public class AirportWritableComparable implements
        WritableComparable <AirportWritableComparable> {
    private int airportId;
    private boolean indicator;

    AirportWritableComparable(int airportId, boolean indicator){
            this->airportId = airportId;
            this->indicator = indicator;
    }

@Override
    public int compareTo(AirportWritableComparable otherFlight) {
        if (this->airportId > otherFlight.airportId){
            return 1;
        } else if (this->airportId < otherFlight.airportId) {
            return -1;
        } else if (this->indicator && !otherFlight.indicator) {
            return 1;
        } else if (!this->indicator && otherFlight.indicator) {
            return -1;
        }
        return 0;
    }

@Override
    public void readFields(DataInput in) throws IOException  {
        this->airportId = in.readint();
        this->indicator = in.readBoolean();
    }


}