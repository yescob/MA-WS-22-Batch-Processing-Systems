package at.fhv.fn.mediantemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.*;

public enum TestContext {
    INSTANCE;

    private String mapInputFileName() {
        return "/mediantemperature/input.txt";
    }

    private String reduceInputFileName() {
        return "/mediantemperature/reducer-input.txt";
    }

    private String mapOutputFileName() {
        return "/mediantemperature/mapper-output.txt";
    }

    private String reduceOutputFileName() {
        return "/mediantemperature/reducer-output.txt";
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

    public List<Pair<YearTemperatureRecord,IntWritable>> expectedMapOutput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(mapOutputFileName()));
        List<Pair<YearTemperatureRecord,IntWritable>> output = new ArrayList<>();
        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");
            String year = keyValue[0];
            String keyTemp = keyValue[1];
            String temperature = keyValue[2];
            output.add(new Pair<>((new YearTemperatureRecord(new Text(year), new IntWritable(Integer.parseInt(keyTemp)))), new IntWritable(Integer.parseInt(temperature))));
        }
        return output;
    }

    public List<Pair<YearTemperatureRecord,List<IntWritable>>> reduceInput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(reduceInputFileName()));

        List<Pair<YearTemperatureRecord, List<IntWritable>>> result = new ArrayList<>();

        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");

            String year = keyValue[0];
            String keyTemp = keyValue[1];

            List<IntWritable> temperatures = new ArrayList<>();
            for (int i = 2; i < keyValue.length; i++) {
                String temperature = keyValue[i];
                temperatures.add(new IntWritable(Integer.parseInt(temperature)));
            }

            result.add(new Pair<>((new YearTemperatureRecord(new Text(year), new IntWritable(Integer.parseInt(keyTemp)))), temperatures));
        }
        return result;
    }

    public List<Pair<Text,IntWritable>> expectedReduceOutput() {
        return getOutput(reduceOutputFileName());
    }
}