package ru.barashkov.distributed;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.util.Iterator;
import java.io.IOException;

public class AirportReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    @Override
    protected void reduce(AirportWritableComparable key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String airportName = iterator.next().toString();
        float minDelayTime = Float.MAX_VALUE;
        float maxDelayTime = 0.0f;
        float sumDelayTime = 0.0f;
        float countDelayed = 0.0f;
        if (!iterator.hasNext()) {
            return;
        }
        while (iterator.hasNext()) {
            float newDelay = Float.parseFloat(iterator.next().toString());
            if (newDelay < minDelayTime){
                minDelayTime = newDelay;
            } else if (newDelay > maxDelayTime) {
                maxDelayTime = newDelay;
            }
            sumDelayTime += newDelay;
            countDelayed += 1.0f;
        }
        context.write(new Text("Name of the airport: " + airportName),
                new Text("\n\tMinimal time of arrival's delay: " + minDelayTime +
                        "\n\tMaximal time of arrival's delay: " + maxDelayTime +
                        "\n\tAverage time of arrival's delay: " + sumDelayTime / countDelayed + "\n\n"));
    }
}