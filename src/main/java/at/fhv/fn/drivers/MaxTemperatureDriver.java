package at.fhv.fn.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import at.fhv.fn.maxtemperature.MaxTemperatureReducer;
import at.fhv.fn.maxtemperature.TemperatureRecordMapper;


/**
 * A simple example that calculates the number of occurrences of words in files.
 * Extends: org.apache.hadoop.conf.Configured the base class for things that may be configured with a Configuration
 * Implements: org.apache.hadoop.util.Tool, A tool interface that supports handling of generic command-line options.
 */
public class MaxTemperatureDriver extends Configured implements Tool {

    /**
     * Constructs a Job object based on the tools configuration, which is used to launch a job.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {

        //Provides access to configuration parameters.
        Configuration conf = new Configuration();

        //GenericOptionsParser is a utility to parse command line arguments generic to the Hadoop framework.
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);

        String[] remainingArgs = optionsParser.getRemainingArgs();


        if (remainingArgs.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());

            //A utility to help run Tools.
            ToolRunner.printGenericCommandUsage(System.err);


            System.exit(2);
        }


        // Create a new Job
        Job job = Job.getInstance(conf, "MaxTemperature");
        job.setJarByClass(getClass());

        // Specify various job-specific parameters

        // Set the input file path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output file path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the mapper and reducer classes
        job.setMapperClass(TemperatureRecordMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        // Set the type of the key in the output
        job.setOutputKeyClass(Text.class);

        // Set the type of the value in the output
        job.setOutputValueClass(IntWritable.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
        System.exit(exitCode);
    }

}