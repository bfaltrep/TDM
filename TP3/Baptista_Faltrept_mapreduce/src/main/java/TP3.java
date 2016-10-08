import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
		
	public static enum WCP
	{
		TOTAL_POP,
		NB_CITIES,
		NB_POP
	}
	
  public static class TP3Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		  String tokens[] = value.toString().split(",");
		  try
		  {
			  if(!tokens[1].matches(""))
				  context.getCounter(WCP.NB_CITIES).increment(1);
			  
			  //donné
			  if(!tokens[4].matches(""))
			  {
				  int pop = Integer.parseInt(tokens[4]);
				  if(pop < 1) return;
				  
				  context.getCounter(WCP.NB_POP).increment(1);
				  context.write(new Text(tokens[1]+","+tokens[5]+","+tokens[6]),new IntWritable(pop));
			  }
		  }
		  catch(Exception e)
		  {
			 e.printStackTrace();
		  }

	  }
  }
  
  public static class TP3Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
    	for(IntWritable val : values)
    	{
    		context.getCounter(WCP.TOTAL_POP).increment(val.get());
    		context.write(key, val);
    	}
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
