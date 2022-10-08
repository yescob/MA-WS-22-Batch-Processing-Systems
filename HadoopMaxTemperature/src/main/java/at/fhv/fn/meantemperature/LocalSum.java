package at.fhv.fn.meantemperature;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

//https://stackoverflow.com/questions/7994438/how-to-customize-writable-class-in-hadoop
public class LocalSum implements Writable {

    private Integer total;
    private Integer count;

    public LocalSum(Integer total, Integer count){
        this.total = total;
        this.count = count;
    }

    public LocalSum(){
        this.total = 0;
        this.count = 0;
    }

    public Integer getTotal() {
        return total;
    }

    public Integer getCount() {
        return count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(total);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total = dataInput.readInt();
        count = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalSum localSum = (LocalSum) o;
        return Objects.equals(total, localSum.total) && Objects.equals(count, localSum.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, count);
    }

    @Override
    public String toString() {
        return "LocalSum{" +
                "total=" + total +
                ", count=" + count +
                '}';
    }
}
