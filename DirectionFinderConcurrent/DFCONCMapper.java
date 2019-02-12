//Mapping to find interaction pairs and RTTs.

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class DFCONCMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    private ArrayList<String> waitlist = new ArrayList<String>();

    @Override
    public void map(LongWritable key, Text value, Context context) 
       throws IOException, InterruptedException {
       String line = value.toString();
       String[] tokens = line.split("\\s");
       String IPaddr1 = new String();
       String IPaddr2 = new String();
        int last_dot;
	// get the two IP address.port fields
        IPaddr1 = tokens[2];
	IPaddr2 = tokens[4];

	// eliminate the port part
	last_dot = IPaddr1.lastIndexOf('.');
	IPaddr1 = IPaddr1.substring(0, last_dot);
	last_dot = IPaddr2.lastIndexOf('.');
	IPaddr2 = IPaddr2.substring(0, last_dot);

    //Find directional pairs
        if (!waitlist.contains(IPaddr1+IPaddr2)) {
        waitlist.add(IPaddr1+IPaddr2);
        waitlist.add(tokens[1]);
        } else {
        int next = waitlist.indexOf(IPaddr1+IPaddr2);
        waitlist.remove(next);
        Double l = Double.parseDouble(waitlist.get(next));
        waitlist.remove(next);
        Double l1 = Double.parseDouble(tokens[1]);
        waitlist.add(IPaddr1+IPaddr2);
        waitlist.add(tokens[1]);
        if (tokens[3].equals(">")) {
        String s = IPaddr1+">"+IPaddr2;
        } else {
        String s = IPaddr1+"<"+IPaddr2;
        }
        l = l1-1;
        String s1 = l.toString();
        context.write(new Text(s), new Text(s1));
    }
  }
}

