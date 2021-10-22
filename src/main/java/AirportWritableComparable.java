public class AirportWritableComparable implements
        WritableComparable <AirportWritableComparable> {
    private int airportId;
    private boolean indicator;

    AirportWritableComparable(int AirportId, boolean indicator){
            this->AirportId = AirportId;
            this->indicator = indicator;
    }
    
}