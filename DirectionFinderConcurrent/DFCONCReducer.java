//groups the RTTs together based on pathway.

import java.util.*;
import java.lang.Math.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class DFCONCReducer
   extends Reducer<Text, Text, Text, Text> {

   private ArrayList<Double> sdlist = new ArrayList<Double>();

 @Override
   public void reduce(Text key, Iterable<Text> values, 
      Context context) throws IOException, InterruptedException {

      int count = 0;
      double min = 0;
      double max = 0;
      double mean = 0;
      double stdev = 0;
      double sum = 0;
      double sdsum = 0;
      boolean first = true;
      sdlist.clear();

for (Text value: values) {
String line = value.toString();
String tokens[] = line.split("\\s");
Double val = Double.parseDouble(tokens[0]);

 if (first) {
          min = val;
          max = val;
          first = false;
          }
          //calculate mean, standard deviation, min, and max here, then report
          //the outside host with these values. You'l need array lists to 
          //manage this. look at flowstats for help with calculations.
          //maybe only do lists with more than X number of values. 5, maybe 10.
          sum = sum + val;
          count++;
          if (val < min) {
              min = val;
          } else if (val > max) {
              max = val;
          }
          sdlist.add(val);
      }
      mean = sum/count;
      //stdev.
      size = sdlist.size();
      for (int i = 0; i < size; i++) {
       double l = (sdlist.get(i));
       l = l - mean;
       l = l*l;
       sdsum = l+sdsum;
      }
      sdsum = sdsum/size;
      stdev = (double) java.lang.Math.sqrt(sdsum);
      if (count >= 50) {
          Text results = new Text("   " + " Min: " + min + " Max: " + max + " Mean: " + mean + " Standard Dev: " + stdev);
          context.write(key,results);
      }
   }
}

