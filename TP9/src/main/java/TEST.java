import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class TEST extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CopyOfKmeans1D.class); //TMP
	
	public static class MapperKMeans extends Mapper<Object, Text, LongWritable, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			LOG.info("LOG DANS LE MAPPER");

			String[] val = value.toString().split(",");

			//if(val[4].matches("Population"))
			{
				StringBuffer tmp = new StringBuffer();
				tmp.append("TEST1\n");
				tmp.append(val[0]+"\n");
				context.write(new LongWritable(1),new Text(tmp.toString()));
			}
		}
	}

	// ----- REDUCER

	public static class ReducerKMeans extends Reducer<LongWritable,Text,Text,Text> {
	
	    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	StringBuffer tmp = new StringBuffer();
	    	tmp.append("TEST2");
	    	for(Text v : values)
	    		tmp.append(v.toString()+"\n");
	    	
			context.write(new Text("1"),new Text(tmp.toString()));
	    }
	  }
	
	
	
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf, "TEST");
	
		job.setNumReduceTasks(1);
	    job.setJarByClass(TP9.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapperClass(MapperKMeans.class);
	    job.setReducerClass(ReducerKMeans.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[5])); 
	   
	    if (job.waitForCompletion(true))
	    	System.out.println("C EST FINIT.");
	    else
		    System.out.println("PROBLEME : CA DEVRAIT ETRE FINIT.");
		return 0;
	    
	}
}
