package at.fhv.fn.mediantemperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearTemperaturePartitioner extends Partitioner<YearTemperatureRecord, Text>{

    @Override
    public int getPartition(YearTemperatureRecord pair, Text text, int numberOfPartitions) {

        return Math.abs(pair.getYear().hashCode() % numberOfPartitions);
    }

    
    
}