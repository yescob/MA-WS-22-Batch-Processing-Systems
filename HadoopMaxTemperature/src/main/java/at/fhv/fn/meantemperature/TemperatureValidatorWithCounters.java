package at.fhv.fn.meantemperature;

import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureValidatorWithCounters {

    private static final Integer MISSING_VALUE = 9999;
    private static final String ALLOWED_QUALITY_NUMBERS_REGEX = "[01459]";
    private static final String COUNTER_GROUP_NAME = "QualityCodesCounter";

    public static boolean checkIfValid(Integer temperature, String qualityRecord, Mapper.Context context){
        boolean isValid = true;

        if(temperature.equals(MISSING_VALUE)){
            context.getCounter(COUNTER_GROUP_NAME, "missing").increment(1);
            isValid = false;
        }
        if(!qualityRecord.matches(ALLOWED_QUALITY_NUMBERS_REGEX)){
            context.getCounter(COUNTER_GROUP_NAME, qualityRecord).increment(1);
            isValid = false;
        }
        return isValid;
    }
}
