package at.fhv.fn.maxtemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        Integer maxTemperature = Integer.MIN_VALUE;
        for(IntWritable v : values){
            if(v.get() > maxTemperature){
                maxTemperature = v.get();
            }
        }
        context.write(key, new IntWritable(maxTemperature));
        
    }
    
}
