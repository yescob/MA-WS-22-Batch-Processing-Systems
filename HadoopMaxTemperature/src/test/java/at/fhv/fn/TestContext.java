package at.fhv.fn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.*;

public enum TestContext {
    INSTANCE;

    private String mapInputFileName() {
        return "/maxtemperaturetest-input.txt";
    }

    private String reduceInputFileName() {
        return "/maxtemperaturetest-reducer-input.txt";
    }

    private String mapOutputFileName() {
        return "/maxtemperaturetest-mapper-output.txt";
    }

    private String reduceOutputFileName() {
        return "/maxtemperaturetest-reducer-output.txt";
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

    public List<Pair<Text,IntWritable>> reduceInput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(reduceInputFileName()));

        List<Pair<Text, IntWritable>> result = new ArrayList<>();

        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");

            String year = keyValue[0];

            List<IntWritable> values = new ArrayList<>();
            Integer maxTemperatue = Integer.parseInt(keyValue[1]);
            for (int i = 2; i < keyValue.length; i++) {
                if(maxTemperatue < Integer.parseInt(keyValue[i]) ){
                    maxTemperatue = Integer.parseInt(keyValue[i]);
                }
            }
            result.add(new Pair<>(new Text(year), new IntWritable(maxTemperatue)));
        }
        return result;
    }

    public List<Pair<Text,IntWritable>> expectedReduceOutput() {
        return getOutput(reduceOutputFileName());
    }
}