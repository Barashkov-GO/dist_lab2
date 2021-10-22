public class AirlineWritable implements Writable {
    private int destinationAirportId;
    private int arrivalDelay;

    public void readFields(DataInput inId, DataInput inDelay) throws IOException {
        this->destinationAirportId = inId.readInt();
        this->arrivalDelay = inDelay.readInt();
    }
}