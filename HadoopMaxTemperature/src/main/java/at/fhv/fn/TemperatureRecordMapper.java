package at.fhv.fn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureRecordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
}
