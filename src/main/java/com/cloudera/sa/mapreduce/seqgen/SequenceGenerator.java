package com.cloudera.sa.mapreduce.seqgen;

import java.io.IOException;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 * 
 */
public class SequenceGenerator {
  public static final String STARTING_SEQ = "custom.starting.seq";
  public static final String NUMBER_OF_REDUCERS = "custom.number.of.reducers";
  public static final String DELIMITER = "custom.delimiter";

  public static class CustomMapper extends
      Mapper<LongWritable, Text, Text, Text> {
    Text newKey = new Text();
    Text newValue = new Text();

    int taskAttemptId;
    int numberOfReducers;

    @Override
    public void setup(Context context) {
      taskAttemptId = context.getTaskAttemptID().getTaskID().getId();
      numberOfReducers = context.getConfiguration().getInt(NUMBER_OF_REDUCERS,
          1);
    }

    long counter = 0;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      long mapSeq = counter++;
      // output record with map level seq
      newKey.set("B" + taskAttemptId + "-" + mapSeq);
      context.write(newKey, value);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      for (int i = 0; i < numberOfReducers; i++) {
        // output record with mapper count
        // We want to send this to every reducer
        newKey.set("A" + i + "-" + taskAttemptId + "-" + counter);
        newValue.set("");
        context.write(newKey, newValue);
      }
    }
  }

  public static class CustomReducer extends
      Reducer<Text, Text, NullWritable, Text> {

    Text newValue = new Text();

    long startingSeq;
    boolean isDonePrep = false;
    SortedSet<String> mapperIdSet = new TreeSet<String>();
    HashMap<String, Long> mapperStartingPoint = new HashMap<String, Long>();
    String delimiter;
    
    @Override
    public void setup(Context context) {
      startingSeq = context.getConfiguration().getInt(STARTING_SEQ,
          1);
      delimiter = context.getConfiguration().get(DELIMITER, "|");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String keyString = key.toString();
      
      if (keyString.startsWith("A")) {
        int startingIndex = keyString.indexOf('-');
        int endingIndex = keyString.indexOf('-', startingIndex + 1);
        
        String mapperId = keyString.substring(startingIndex+1, endingIndex);
        
        mapperIdSet.add(mapperId);
        mapperStartingPoint.put(mapperId, Long.parseLong(keyString.substring(endingIndex+1)));
        
        System.out.println("PrePrep:" + mapperId + ":" + Long.parseLong(keyString.substring(endingIndex+1)) + " " + keyString);
        
      } else {
        if (isDonePrep == false) {
          isDonePrep = true;
          long counter = 0;
          for (String mapperId: mapperIdSet) {
            
            long counterHolder = mapperStartingPoint.get(mapperId);
            mapperStartingPoint.put(mapperId, counter);
            
            counter += counterHolder;
            
            System.out.println("Prep:" + mapperId + ":" + counter);
          }
        }
        
        int startingIndex = keyString.indexOf('-');
        String mapperId = keyString.substring(1, startingIndex);
        
        System.out.println("dataRow:" + mapperId + ":" + mapperStartingPoint.get(mapperId));
        
        long mapperSeq = Long.parseLong(keyString.substring(startingIndex+1)) + mapperStartingPoint.get(mapperId) + startingSeq;
        
        System.out.println("result:" + mapperSeq);
        
        for (Text value: values) {
          newValue.set(mapperSeq + delimiter + value.toString());
        }
        
        context.write(NullWritable.get(), newValue);
      }
    }
  }
  
  public static class CustomPartitioner  extends Partitioner<Text, Text> implements Configurable  {

    
    public void setConf(Configuration conf) {
    }
    
    @Override
    public int getPartition(Text key, Text value, int numPartitions)
    {
      if (numPartitions == 1)
      {
        return 0;
      } else
      {
        String keyString = key.toString();

        if (keyString.startsWith("B")) {
          //We have a record value
          int dashIndex = keyString.indexOf('-');
          String partitionKey;
          partitionKey = keyString.substring(1, dashIndex);
          
          int result = (partitionKey).hashCode() % numPartitions;
          return Math.abs(result);    
        } else {
          //We have a count value
          int dashIndex = keyString.indexOf('-');
          String partitionKey;
          partitionKey = keyString.substring(1, dashIndex);
          return Integer.parseInt(partitionKey);
        }
      }
    }

    public Configuration getConf() {
      // TODO Auto-generated method stub
      return null;
    }


  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    long startingSeq;
    int numberOfReducers;
    String input;
    String output;
    String delimiter;
    try {
      input = args[0];
      output = args[1];
      startingSeq = Long.parseLong(args[2]);
      numberOfReducers = Integer.parseInt(args[3]);
      delimiter = args[4];
    } catch (Exception e) {
      System.out
          .println("SequenceGenerator <input> <output> <startingSeq> <numberOfReducers> <delimiter>");
      System.out.println("SequenceGenerator ./input ./output 100 2 |");
      return;
    }
 
    // Create job
    Job job = new Job();

    job.getConfiguration().set(STARTING_SEQ, Long.toString(startingSeq));
    job.getConfiguration().set(NUMBER_OF_REDUCERS,
        Integer.toString(numberOfReducers));
    job.getConfiguration().set(DELIMITER, delimiter);

    job.setJarByClass(SequenceGenerator.class);
    // Define input format and path
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(input));

    // Define output format and path
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(output));

    // Define the mapper and reducer
    job.setMapperClass(CustomMapper.class);
    job.setReducerClass(CustomReducer.class);
    job.setPartitionerClass(CustomPartitioner.class);

    // Define the key and value format
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(numberOfReducers);

    // Exit
    job.waitForCompletion(true);
  }
}
