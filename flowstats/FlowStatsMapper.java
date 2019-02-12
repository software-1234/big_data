// map function for application to compute the mean and
// standard deviation for the number of bytes sent by
// each IP address in the IP address pair that defines
// a flow.  
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowStatsMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split("\\s");
    String IPaddr1 = new String();
    String IPaddr2 = new String();
    String FlowTuple = new String();  //IP address pair composite key
    String FlowCount = new String();  //output two values for each ADU
    int last_dot;
	// get the two IP address.port fields
        IPaddr1 = tokens[2];
	IPaddr2 = tokens[4];

	// eliminate the port part
	last_dot = IPaddr1.lastIndexOf('.');
	IPaddr1 = IPaddr1.substring(0, last_dot);
	last_dot = IPaddr2.lastIndexOf('.');
	IPaddr2 = IPaddr2.substring(0, last_dot);

        // create a composite key for a flow as an IP address pair string
        FlowTuple = IPaddr1 + ":" + IPaddr2;

         if (tokens[3].equals(">")) {
             //bytes sent addr1 -> addr2, zero sent by addr2
             FlowCount = tokens[5] + " " + "0";
         }
         else if (tokens[3].equals("<")) {
             //bytes sent addr2 -> addr2, zero sent by addr1
	     FlowCount = "0" + " " + tokens[5];
              }

        // output the key, value pairs where the key is an
        // IP address Flow-tuple and the value is a string giving
        // counts for the flow.
	 context.write(new Text(FlowTuple), new Text(FlowCount));
  }
}
