package at.fhv.fn.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.*;

public enum TestContext {
    INSTANCE;

    private String mapInputFileName() {
        return "/maxTemperature/meantemperaturetest-input.txt";
    }

    private String reduceInputFileName() {
        return "/maxTemperature/meantemperaturetest-reducer-input.txt";
    }

    private String mapOutputFileName() {
        return "/maxTemperature/meantemperaturetest-mapper-output.txt";
    }

    private String reduceOutputFileName() {
        return "/maxTemperature/meantemperaturetest-reducer-output.txt";
    }

    private List<Pair<Text,IntWritable>> getOutput(String fileName){
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(fileName));
        List<Pair<Text,IntWritable>> output = new ArrayList<>();
        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");
            String year = keyValue[0];
            String temperature = keyValue[1];
            output.add(new Pair<>(new Text(year), new IntWritable(Integer.parseInt(temperature))));
        }
        return output;
    }

    public List<Pair<LongWritable, Text>> mapInput() {

        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(mapInputFileName()));
        List<Pair<LongWritable, Text>> input = new ArrayList<>();

        while(scanner.hasNext()) {
            String line = scanner.nextLine();
            input.add(new Pair<>(new LongWritable(0), new Text(line)));
        }
        return input;
    }

    public List<Pair<Text,IntWritable>> expectedMapOutput() {
        return getOutput(mapOutputFileName());
    }

    public List<Pair<Text,List<IntWritable>>> reduceInput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(reduceInputFileName()));

        List<Pair<Text, List<IntWritable>>> result = new ArrayList<>();

        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");

            String year = keyValue[0];

            List<IntWritable> temperatures = new ArrayList<>();
            for (int i = 1; i < keyValue.length; i++) {
                String temperature = keyValue[i];
                temperatures.add(new IntWritable(Integer.parseInt(temperature)));
            }

            result.add(new Pair<>(new Text(year), temperatures));
        }
        return result;
    }

    public List<Pair<Text,IntWritable>> expectedReduceOutput() {
        return getOutput(reduceOutputFileName());
    }
}