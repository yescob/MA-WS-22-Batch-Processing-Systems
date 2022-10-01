package at.fhv.fn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MaxTemperatureTest {

    private TestContext tc = TestContext.INSTANCE;

    private Mapper mapper;
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private Reducer reducer;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        mapper = new TemperatureRecordMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        reducer = new MaxTemperatureReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException, InterruptedException {

        //Add inputs
        mapDriver.addAll(tc.mapInput());

        //Add expected outputs for the test
        mapDriver.withAllOutput(tc.expectedMapOutput());

        //Runs the test and validates the results
        mapDriver.runTest();
    }


    @Test
    public void testReduce() throws Exception {
        //Add inputs
        reduceDriver.addAllOutput(tc.reduceInput());

        //Add expected outputs for the test
        reduceDriver.withAllOutput(tc.expectedReduceOutput());

        //Runs the test and validates the results
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        //Add inputs
        mapReduceDriver.addAll(tc.mapInput());

        //Add expected outputs for the test
        mapReduceDriver.addAllOutput(tc.expectedReduceOutput());

        //Runs the test and validates the results
        mapReduceDriver.runTest();
    }


}
