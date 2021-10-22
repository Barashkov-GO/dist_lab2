public class AirportWritableComparable implements
        WritableComparable <AirportWritableComparable> {
    private int airportId;
    private boolean indicator;

    AirportWritableComparable(int airportId, boolean indicator){
            this->airportId = airportId;
            this->indicator = indicator;
    }

    public int compareTo(MyWritableComparable o) {
        int thisValue = this.value;
        int thatValue = o.value;
        return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

}