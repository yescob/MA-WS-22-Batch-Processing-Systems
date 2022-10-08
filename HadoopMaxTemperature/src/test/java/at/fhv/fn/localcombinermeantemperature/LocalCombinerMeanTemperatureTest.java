package at.fhv.fn.localcombinermeantemperature;

import at.fhv.fn.meantemperature.LocalCombinerMeanTemperatureMapper;
import at.fhv.fn.meantemperature.LocalCombinerMeanTemperatureReducer;
import at.fhv.fn.meantemperature.LocalSum;
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

import static org.junit.Assert.assertEquals;

public class LocalCombinerMeanTemperatureTest {

    private static final String COUNTER_GROUP_NAME = "QualityCodesCounter";

    private TestContext tc = TestContext.INSTANCE;

    private Mapper mapper;
    private MapDriver<LongWritable, Text, Text, LocalSum> mapDriver;

    private Reducer reducer;
    private ReduceDriver<Text, LocalSum, Text, IntWritable> reduceDriver;

    private MapReduceDriver<LongWritable, Text, Text, LocalSum, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        mapper = new LocalCombinerMeanTemperatureMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        reducer = new LocalCombinerMeanTemperatureReducer();
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
        assertEquals("Missing Value", 1, mapDriver.getCounters().findCounter(COUNTER_GROUP_NAME, "missing").getValue());
        assertEquals("7", 3, mapDriver.getCounters().findCounter(COUNTER_GROUP_NAME, "7").getValue());
        assertEquals("8", 1, mapDriver.getCounters().findCounter(COUNTER_GROUP_NAME, "8").getValue());
        assertEquals("2", 1, mapDriver.getCounters().findCounter(COUNTER_GROUP_NAME, "2").getValue());
        assertEquals("3", 1, mapDriver.getCounters().findCounter(COUNTER_GROUP_NAME, "3").getValue());
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
        //Add inputs
        mapReduceDriver.addAll(tc.mapInput());

        //Add expected outputs for the test
        mapReduceDriver.addAllOutput(tc.expectedReduceOutput());

        //Runs the test and validates the results
        mapReduceDriver.runTest();
    }


}

