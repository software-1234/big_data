//Main function to creat the time series of sequential data based on IP
//address.

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DFCONC {

    public static void main(String [] args) throws Exception {
        if (args.length!=2) {
            System.err.println("Usage: HostCount <input path> <output path>");
            System.exit(-1);
         }
        //telling JobTracker about the job.
        Job job = Job.getInstance();
        job.setJarByClass(DFCONC.class);
        job.setJobName("Direction Finder");

        //set the input and output paths (the args).
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //set the Mapper and Reducer classes to be called.
        job.setMapperClass(DFCONCMapper.class);
        job.setReducerClass(DFCONCReducer.class);
        job.setNumReduceTasks(20);

        //set the format of keys/values.
        //one back and forth interaction, with RTT. Text = outside IP,
        //LongWriteable = RTT.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //list host x with all its RTTs, standard dev, average, min and max.
        //Text: host x, Text: average RTT, std dev, min, max.
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);

        //submit job and wait.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

