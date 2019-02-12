// main function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RandomFilter {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: HostCount <input path> <output path>");
      System.exit(-1);
    }
    //define the job to the JobTracker
    Job job = Job.getInstance();
    job.setJarByClass(RandomFilter.class);
    job.setJobName("Random Filter");

    // set the input and output paths (passed as args)
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // set the Mapper class (default identity reducer used)   
    job.setMapperClass(RandomFilterMapper.class);

    // set the format of the keys and values
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    // submit the job and wait for its completion    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
