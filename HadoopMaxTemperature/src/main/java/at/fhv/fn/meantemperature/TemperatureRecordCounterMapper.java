package at.fhv.fn.meantemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class TemperatureRecordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Integer START_POSITION_YEAR = 16;
    private static final Integer END_POSITION_YEAR = 19;
    private static final Integer START_POSITION_TEMPERATURE = 88;
    private static final Integer END_POSITION_TEMPERATURE = 92;
    private static final Integer POSITION_QUALITY_CODE = 93;

    //Start posistion -1 because Array start at 0, End posistion can be the same as substring is endIndex-1
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String record = value.toString();

        String year = record.substring(START_POSITION_YEAR-1,END_POSITION_YEAR); // year is key for reducer

        Integer temperature = Integer.parseInt(record.substring(START_POSITION_TEMPERATURE-1,END_POSITION_TEMPERATURE));
        String qualityRecord = record.substring(POSITION_QUALITY_CODE-1, POSITION_QUALITY_CODE);

        if(TemperatureValidatorWithCounters.checkIfValid(temperature, qualityRecord, context)){
            context.write(new Text(year), new IntWritable(temperature));
        }
    }

}
