import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportComparator extends WritableComparator {

    AirportComparator(){

    }

    @Override
    public int compare(WritableComparable firstFlight, WritableComparable secondFlight) {
        AirportWritableComparable firstFlightWC = new AirportWritableComparable(firstFlight);
        AirportWritableComparable secondFlightWC = new AirportWritableComparable(secondFlight);
        return firstFlightWC.compareId(secondFlightWC);
    }
}