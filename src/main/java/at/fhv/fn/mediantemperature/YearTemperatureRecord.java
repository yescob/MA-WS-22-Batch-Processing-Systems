package at.fhv.fn.mediantemperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class YearTemperatureRecord implements WritableComparable{

    String year;
    Integer temperature;

    public YearTemperatureRecord(String year, Integer temperature){
        this.year = year;
        this.temperature = temperature;
    }

    public YearTemperatureRecord(){};

    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int compareTo(Object o) {
        // TODO Auto-generated method stub
        return 0;
    }

}
