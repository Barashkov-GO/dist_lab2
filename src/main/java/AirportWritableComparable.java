public class AirportWritableComparable implements
        WritableComparable <AirportWritableComparable> {
    private int airportId;
    private boolean indicator;

    AirportWritableComparable(int airportId, boolean indicator){
            this->airportId = airportId;
            this->indicator = indicator;
    }

    public int compareTo(AirportWritableComparable otherFlight) {
        
        return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

}