package at.fhv.fn;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureRecordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //https://www1.ncdc.noaa.gov/pub/data/ish/ish-format-document.pdf 
    private static final Integer START_POSITION_YEAR = 16;
    private static final Integer END_POSITION_YEAR = 19;
    private static final Integer START_POSITION_TEMPERATURE = 88;
    private static final Integer END_POSITION_TEMPERATURE = 92;
    private static final Integer POSITION_QUALITY_CODE = 93;
    private static final Integer MISSING_VALUE = 9999;
    private static final String ALLOWED_QUALITY_NUMBERS_REGEX = "[01459]";

    //Start posistion -1 because Array start at 0, End posistion can be the same as substring is endIndex-1    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String record = value.toString();
    
        String year = record.substring(START_POSITION_YEAR-1,END_POSITION_YEAR); // year is key for reducer
        
        Integer temperature = Integer.parseInt(record.substring(START_POSITION_TEMPERATURE-1,END_POSITION_TEMPERATURE));
        String qualityRecord = record.substring(POSITION_QUALITY_CODE-1, POSITION_QUALITY_CODE);

        // only consider valid temperature records
        if(!temperature.equals(MISSING_VALUE) && qualityRecord.matches(ALLOWED_QUALITY_NUMBERS_REGEX)){
            context.write(new Text(year), new IntWritable(temperature));
        }
    }
    
}
