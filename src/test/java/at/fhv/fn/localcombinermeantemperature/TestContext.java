package at.fhv.fn.localcombinermeantemperature;

import at.fhv.fn.meantemperature.LocalSum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public enum TestContext {
    INSTANCE;

    private String mapInputFileName() {
        return "/meantemperature/meantemperaturetest-input.txt";
    }

    private String reduceInputFileName() {
        return "/meantemperature/meantemperaturetest-combined-reducer-input.txt";
    }

    private String mapOutputFileName() {
        return "/meantemperature/meantemperaturetest-combined-mapper-output.txt";
    }

    private String reduceOutputFileName() {
        return "/meantemperature/meantemperaturetest-reducer-output.txt";
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

    public List<Pair<Text,LocalSum>> expectedMapOutput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(mapOutputFileName()));

        List<Pair<Text, LocalSum>> result = new ArrayList<>();

        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");

            String year = keyValue[0];

            String localSum[] = keyValue[1].split(";");
            String total = localSum[0];
            String count = localSum[1];
            result.add(new Pair<>(new Text(year) , new LocalSum(Integer.parseInt(total), Integer.parseInt(count))));
        }
        return result;
    }

    public List<Pair<Text, List<LocalSum>>> reduceInput() {
        Scanner scanner = new Scanner(TestContext.class.getResourceAsStream(reduceInputFileName()));

        List<Pair<Text, List<LocalSum>>> result = new ArrayList<>();

        while(scanner.hasNext()){
            String keyValue[] = scanner.nextLine().split(",");

            String year = keyValue[0];

            List<LocalSum> localSums = new ArrayList<>();
            for (int i = 1; i < keyValue.length; i++) {
                String localSum[] = keyValue[i].split(";");
                String total = localSum[0];
                String count = localSum[1];
                localSums.add(new LocalSum(Integer.parseInt(total), Integer.parseInt(count)));
            }

            result.add(new Pair<>(new Text(year), localSums));
        }
        return result;
    }

    public List<Pair<Text,IntWritable>> expectedReduceOutput() {
        return getOutput(reduceOutputFileName());
    }
}