package ru.barashkov.distributed;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public class AirportWritableComparable implements
        WritableComparable <AirportWritableComparable> {
    private int airportId;
    private int indicator;

    AirportWritableComparable() {}

    AirportWritableComparable(int airportId, int indicator){
        this.airportId = airportId;
        this.indicator = indicator;
    }

    public int getAirportId(){
        return this.airportId;
    }

    public int getIndicator() {
        return this.indicator;
    }

    @Override
    public int compareTo(AirportWritableComparable otherFlight) {
        if (this.airportId > otherFlight.getAirportId()){
            return 1;
        } else if (this.airportId < otherFlight.getAirportId()) {
            return -1;
        } else if (this.indicator > otherFlight.getIndicator()) {
            return 1;
        } else if (this.indicator < otherFlight.getIndicator()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException  {
        this.airportId = in.readInt();
        this.indicator = in.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException  {
        dataOutput.writeInt(this.airportId);
        dataOutput.writeInt(this.indicator);
    }

    public int compareId(AirportWritableComparable otherFlight) {
        if (this.airportId > otherFlight.getAirportId()) {
            return 1;
        }
        if (this.airportId < otherFlight.getAirportId()) {
            return -1;
        }
        return 0;
    }
}