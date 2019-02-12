//groups the RTTs together based on pathway.

import java.util.*;
import java.lang.Math.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class DFSEQReducer
   extends Reducer<Text, DoubleWritable, Text, Text> {

   private ArrayList<Long> meanlist = new ArrayList<Long>();
   private ArrayList<Double> sdlist = new ArrayList<Double>();

   @Override
   public void reduce(Text key, Iterable<DoubleWritable> values, 
      Context context) throws IOException, InterruptedException {

      int count = 0;
      double min = 0;
      double max = 0;
      double mean = 0;
      double stdev = 0;
      double sum = 0;
      double sdsum = 0;
      boolean first = true;
      meanlist.clear();
      sdlist.clear();

      for (DoubleWritable value: values) {
          if (first) {
          min = value.get();
          max = value.get();
          first = false;
          }
          //calculate mean, standard deviation, min, and max here, then report
          //the outside host with these values. You'l need array lists to 
          //manage this. look at flowstats for help with calculations.
          //maybe only do lists with more than X number of values. 5, maybe 10.
          sum = sum + value.get();
          count++;
          if (value.get() < min) {
              min = value.get();
          } else if (value.get() > max) {
              max = value.get();
          }
          sdlist.add(value.get());
      }
      mean = sum/count;
      //stdev.
      for (int i = 0; i < sdlist.size(); i++) {
       double l = (sdlist.get(i));
       l = l - mean;
       l = l*l;
       sdsum = l+sdsum;
       sdlist.set(i, l);
      }
      sdsum = sdsum/sdlist.size();
      stdev = (double) java.lang.Math.sqrt(sdsum);
      if (count > 100) {
          Text results = new Text("   " + " Min: " + min + " Max: " + max + " Mean: " + mean + " Standard Dev: " + stdev);
          context.write(key,results);
      }
      }
}
