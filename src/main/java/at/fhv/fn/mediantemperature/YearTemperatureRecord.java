package at.fhv.fn.mediantemperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearTemperatureRecord implements WritableComparable<YearTemperatureRecord>{

    private Text year;
    private IntWritable temperature;

    public YearTemperatureRecord(Text year, IntWritable temperature){
        this.year = year;
        this.temperature = temperature;
    }

    public YearTemperatureRecord(){
        this(new Text(), new IntWritable());
    };

    public Text getYear(){
        return this.year;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        year.readFields(arg0);
        temperature.readFields(arg0);        
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        year.write(arg0);
        temperature.write(arg0);   
    }

    @Override
    public int compareTo(YearTemperatureRecord o) {
        int cmp = year.compareTo(o.getYear());
        if(cmp != 0){
            return cmp;
        }
        return temperature.compareTo(o.temperature);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearTemperatureRecord other = (YearTemperatureRecord) o;
        return Objects.equals(year, other.year) && Objects.equals(temperature, other.temperature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, temperature);
    }

    @Override
    public String toString() {
        return "YearTemperatureRecord [year=" + year + ", temperature=" + temperature + "]";
    }

    

    

}
