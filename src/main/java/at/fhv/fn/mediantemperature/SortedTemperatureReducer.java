package at.fhv.fn.mediantemperature;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortedTemperatureReducer extends Reducer<YearTemperatureRecord, IntWritable, Text, IntWritable> {

    //Nearest-rank Method for Percentile
    public void reduce(YearTemperatureRecord key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        Integer percentile = Integer.parseInt(context.getConfiguration().get("Percentile"));

        ArrayList<Integer> temperatures = new ArrayList<>();

        for(IntWritable v : values){
            temperatures.add(v.get());
        }

        Integer result = 0;
        Integer percentilePosition = 0;
        double percent = percentile/100.0;
        double ordinalRank = temperatures.size()*percent;

        if(ordinalRank % 1 == 0){
            percentilePosition = (int) ordinalRank;
            if(percentilePosition >= temperatures.size()){
                percentilePosition = temperatures.size() -1;
            }
            result = (temperatures.get(percentilePosition) + temperatures.get(percentilePosition-1))/2;
        } else {
            result = temperatures.get((int) Math.floor(ordinalRank));
        }
        context.write(new Text(key.getYear()), new IntWritable(result));

    }
    
}
