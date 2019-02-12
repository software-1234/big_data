// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;


public class RandomFilterMapper
  extends Mapper<LongWritable, Text, NullWritable, Text> {

    //Instance of random number generator with initial seed value
    //Change seed value to get different random sample (seed should be prime)
    //See https://primes.utm.edu/lists/small/10000.txt for list

        Random randv = new Random(2063); 
        Double fraction = 0.00001;  //sample 1 per 100000
    
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      //generate new random number in [0,1]; output line if < desired fraction
      if (randv.nextDouble() < fraction) 
          context.write(NullWritable.get(), value);
  }
}

