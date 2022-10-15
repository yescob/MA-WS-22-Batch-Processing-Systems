package at.fhv.fn.mediantemperature;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortedTemperatureReducer extends Reducer<YearTemperatureRecord, IntWritable, Text, IntWritable> {

    public void reduce(YearTemperatureRecord key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        ArrayList<Integer> temperatures = new ArrayList<>();

        for(IntWritable v : values){
            temperatures.add(v.get());
        }

        Integer middle = temperatures.size()/2;
        if (temperatures.size()%2 == 1) {
            middle = temperatures.get(temperatures.size() / 2);            
        } else {
            middle = (temperatures.get(temperatures.size()/2) + temperatures.get(temperatures.size()/2 - 1))/2; 
        }      
        context.write(new Text(key.getYear()), new IntWritable(middle));

    }
    
}
