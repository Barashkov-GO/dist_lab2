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

        while (iterator.hasNext()) {
            float newDelay = Float.parseFloat(iterator.next());
            (newDelay < minTime) ? minTime = newDelay;
            (newDelay > maxTime) ? maxTime = newDelay;
            
            Text call = iterator.next();
            Text outValue = new Text(call.toString() + "\t" + systemInfo.toString());
            context.write(key.getFirst(), outValue);
        }
    }
}