import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.util.Iterator;
import java.io.IOException;

public class AirportReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    @Override
    protected void reduce(AirportWritableComparable key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String airportName = iterator.toString();
        float minTime = float.MAX_VALUE;
        float maxTime = 0.0f;
        float sumTime = 0.0f;
        int count = 0;

        while (iterator.hasNext()) {
            float newDelay = Float.parseFloat(iterator.next());
            (newDelay < minTime) ? minTime = newDelay;
            (newDelay > maxTime) ? maxTime = newDelay;
            sumTime += newDelay;
            count += 1;
        }
        context.write(new Text("Name of the airport: " + airportName), new Text())
    }
}