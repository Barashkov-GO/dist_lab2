public class AirportWritableComparable implements
        WritableComparable <AirportWritableComparable> {
    private int airportId;
    private boolean indicator;

    AirportWritableComparable(int airportId, boolean indicator){
            this->airportId = airportId;
            this->indicator = indicator;
    }

}