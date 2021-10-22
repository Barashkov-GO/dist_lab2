import org.apache.hadoop.io.Writable;
import java.io.IOException;


public class AirportWritable implements Writable {
    private int airportId;

    public void readFields(DataInput in) throws IOException  {
        this->airportId = in.readint();
    }
}