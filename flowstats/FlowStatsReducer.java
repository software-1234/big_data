// reducer function for application to compute the mean and
// standard deviation for the number of bytes sent by
// each IP address in the IP address pair that defines
// a flow.  Compute results only for flows that send 10
// or more ADUs in both directions.  

import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowStatsReducer
  extends Reducer<Text, Text, Text, Text> {

  //define ArrayLists to cache byte counts from each of the ADUs
  //in a flow.  This local cacheing is necessary because the
  //Iterable provided by the MapReduce framework cannot be
  //iterated more than once. bytes1_2 caches byte counts sent
  //from IP address 1 to 2 and bytes2_1 caches those in the
  //opposite direction. 

  private ArrayList<Float> bytes1_2 = new ArrayList<Float>();
  private ArrayList<Float> bytes2_1 = new ArrayList<Float>();

  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

      float sum1_2 = 0;  //total bytes sent IP address 1 to 2
      float sum2_1 = 0;  //total bytes sent IP address 2 to 1
      float count1_2 = 0; //ADU count IP address 1 to 2
      float count2_1 = 0; //ADU count IP address 1 to 2
      float bytes1 = 0;
      float bytes2 = 0;

      // clear the cache for each new flow key
      bytes1_2.clear();
      bytes2_1.clear();

      // iterate through all the values with a common key (FlowTuple)
      for ( Text value : values) {
          String line = value.toString();
          String[] tokens = line.split("\\s");
          bytes1 = (float)Long.parseLong(tokens[0]);
          bytes2 = (float)Long.parseLong(tokens[1]);
          // only one of bytes1 and bytes2 will be non-zero
          if (bytes1 > 0) {
             count1_2 += 1;
             sum1_2 += bytes1;
             bytes1_2.add(bytes1);  //cache byte count
	  }
          if (bytes2 > 0) {
             count2_1 += 1;
             sum2_1 += bytes2;
             bytes2_1.add(bytes2);  //cache byte count
	  }
      }
      if ((count1_2 >= 10) && (count2_1 >= 10)){
          // calculate the mean
	  float mean1_2 = sum1_2/count1_2;
          float mean2_1 = sum2_1/count2_1;
          // calculate standard deviation
          float sumOfSquares1 = 0.0f;
          float sumOfSquares2 = 0.0f;
          // compute sum of square differences from mean
          for (Float f : bytes1_2) {
              sumOfSquares1 += (f - mean1_2) * (f - mean1_2);
          }

          for (Float f : bytes2_1) {
              sumOfSquares2 += (f - mean2_1) * (f - mean2_1);
          }
          // compute the variance and take the square root to get standard deviation
          float stddev1_2 = (float) Math.sqrt(sumOfSquares1 / (count1_2 - 1));        
          float stddev2_1 = (float) Math.sqrt(sumOfSquares2 / (count2_1 - 1));
          // output byte mean and standard deviation for both IP addresses       
          String FlowStats = Float.toString(mean1_2) + " " + Float.toString(stddev1_2) + " " + Float.toString(mean2_1) + " " + Float.toString(stddev2_1);
          context.write(key, new Text(FlowStats));
      }
  }
}
