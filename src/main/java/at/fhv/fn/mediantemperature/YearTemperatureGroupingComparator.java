package at.fhv.fn.mediantemperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearTemperatureGroupingComparator extends WritableComparator {

    public YearTemperatureGroupingComparator(){
        super(YearTemperatureRecord.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2){
        YearTemperatureRecord o1 = (YearTemperatureRecord) wc1;
        YearTemperatureRecord o2 = (YearTemperatureRecord) wc2;
        return o1.getYear().compareTo(o2.getYear());
    }
    
}
