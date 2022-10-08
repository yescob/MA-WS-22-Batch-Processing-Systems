package at.fhv.fn.meantemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveMeanTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        Integer sumOfAllRecords = 0;      
        Integer numberOfRecords = 0;
        for(IntWritable v : values){
          sumOfAllRecords = sumOfAllRecords + v.get();
          numberOfRecords++;
        }
        context.write(key, new IntWritable(sumOfAllRecords/numberOfRecords));
        
    }
    
}
