import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class outboundCONCsortMapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {

@Override
    public void map(LongWritable key, Text value, Context context) 
       throws IOException, InterruptedException {
       String line = value.toString();
       String[] tokens = line.split("\\s");
       if (tokens[3].equals(">")) {
         context.write(NullWritable.get(), value);
       }
    }
 }
