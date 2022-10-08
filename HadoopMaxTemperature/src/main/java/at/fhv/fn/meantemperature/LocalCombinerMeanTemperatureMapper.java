package at.fhv.fn.meantemperature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalCombinerMeanTemperatureMapper extends Mapper<LongWritable, Text, Text, LocalSum> {

    private static final Integer START_POSITION_YEAR = 16;
    private static final Integer END_POSITION_YEAR = 19;
    private static final Integer START_POSITION_TEMPERATURE = 88;
    private static final Integer END_POSITION_TEMPERATURE = 92;
    private static final Integer POSITION_QUALITY_CODE = 93;

    private HashMap<String,LocalSum> sumsPerYear;
    private static final Integer FLUSH_SIZE = 100;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LocalSum>.Context context) throws IOException, InterruptedException {
        sumsPerYear = new HashMap<>();
    }

    //Start posistion -1 because Array start at 0, End posistion can be the same as substring is endIndex-1
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String record = value.toString();

        String year = record.substring(START_POSITION_YEAR-1,END_POSITION_YEAR); // year is key for reducer

        Integer temperature = Integer.parseInt(record.substring(START_POSITION_TEMPERATURE-1,END_POSITION_TEMPERATURE));
        String qualityRecord = record.substring(POSITION_QUALITY_CODE-1, POSITION_QUALITY_CODE);

        if(TemperatureValidatorWithCounters.checkIfValid(temperature, qualityRecord, context)){
            if(sumsPerYear.size() >= FLUSH_SIZE) {
                flush(context);
                sumsPerYear = new HashMap<>();
            }
            LocalSum ls = sumsPerYear.getOrDefault(year, new LocalSum());
            sumsPerYear.put(year, new LocalSum(ls.getTotal()+temperature, ls.getCount() + 1));
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, LocalSum>.Context context) throws IOException, InterruptedException {
        flush(context);
    }

    private void flush(Context context) throws IOException, InterruptedException {
        for(Map.Entry<String, LocalSum> entry : sumsPerYear.entrySet()){
            context.write(new Text(entry.getKey()), entry.getValue());
        }
    }
}
