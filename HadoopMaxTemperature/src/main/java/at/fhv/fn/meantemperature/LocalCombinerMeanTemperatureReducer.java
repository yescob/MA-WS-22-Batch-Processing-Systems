package at.fhv.fn.meantemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LocalCombinerMeanTemperatureReducer extends Reducer<Text, LocalSum, Text, IntWritable>{

    public void reduce(Text key, Iterable<LocalSum> values, Context context) throws IOException, InterruptedException{

        Integer sumOfAllRecords = 0;
        Integer numberOfRecords = 0;
        for(LocalSum ls : values){
            sumOfAllRecords = sumOfAllRecords + ls.getTotal();
            numberOfRecords = numberOfRecords + ls.getCount();
        }
        context.write(key, new IntWritable(sumOfAllRecords/numberOfRecords));
    }
}
