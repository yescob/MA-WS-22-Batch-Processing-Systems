package at.fhv.fn.mediantemperature;

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


public class MedianTemperatureTest {

    private TestContext tc = TestContext.INSTANCE;

    private Mapper mapper;
    private MapDriver<LongWritable, Text, YearTemperatureRecord, IntWritable> mapDriver;

    private Reducer reducer;
    private ReduceDriver<YearTemperatureRecord, IntWritable, Text, IntWritable> reduceDriver;

    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        mapper = new SortedTemperatureMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        reducer = new SortedTemperatureReducer();
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
        reduceDriver.addAll(tc.reduceInput());

        //Add expected outputs for the test
        reduceDriver.withAllOutput(tc.expectedReduceOutput());

        //Runs the test and validates the results
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver.withKeyGroupingComparator(new YearTemperatureGroupingComparator());

        //Add inputs
        mapReduceDriver.addAll(tc.mapInput());

        //Add expected outputs for the test
        mapReduceDriver.addAllOutput(tc.expectedReduceOutput());

        //Runs the test and validates the results
        mapReduceDriver.runTest();
    }


}

