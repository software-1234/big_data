import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class inboundCONCsort {
public static void main(String [] args) throws Exception {
        if (args.length!=2) {
            System.err.println("Usage: Inbound CONC sort <input path> <output path>");
            System.exit(-1);
         }
//telling JobTracker about the job.
        Job job = Job.getInstance();
        job.setJarByClass(inboundCONCsort.class);
        job.setJobName("Inbound Sort");

 //set the input and output paths (the args).
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//set the Mapper and Reducer classes to be called.
        job.setMapperClass(inboundCONCsortMapper.class);
        job.setNumReduceTasks(1);

//set the format of keys/values.
        //one back and forth interaction, with RTT. Text = outside IP,
        //LongWriteable = RTT.
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //list host x with all its RTTs, standard dev, average, min and max.
        //Text: host x, Text: average RTT, std dev, min, max.
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);

        //submit job and wait.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


