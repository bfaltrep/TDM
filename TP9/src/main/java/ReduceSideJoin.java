import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {
	
	public static class TaggedValue implements Writable{

		private boolean isATown ;
		private String _content ;
		
		public TaggedValue(){
		}
		
		public TaggedValue(boolean isATown, String content){
			this.isATown=isATown;
			this._content=content;
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeBoolean(isATown);
			out.writeUTF(_content);
		}

		public void readFields(DataInput in) throws IOException {
			isATown = in.readBoolean();
			_content = in.readUTF();
		}
		
		public boolean getIsATown(){
			return isATown;
		}
		
		public String getMyContent(){
			return _content;
		}
	}
	
  public static class MapperCities extends Mapper<Object, Text, Text, TaggedValue>{
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String[] val = value.toString().split(",");
		  if(!val[3].matches("Region")){
		  	  context.write(new Text(val[0].toUpperCase()+","+val[3]),new TaggedValue(true,val[1]));
		  }
	  }
  }
  
  public static class MapperRegion extends Mapper<Object, Text, Text, TaggedValue>{
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String[] val = value.toString().split(",");
		  context.write(new Text(val[0]+","+val[1]),new TaggedValue(false,val[2]));
	  }
  }
  
  public static class RSJReducer extends Reducer<Text,TaggedValue,Text,Text> {
    public void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
    	String region = "";
    	
    	for(TaggedValue val : values)
    		if(!val.getIsATown()){
    			region = val.getMyContent();
    			break;
    		}

    	for(TaggedValue val : values)
    		if(val.getIsATown())
    			context.write(new Text(region),new Text(val.getMyContent()));
    }
    
  }
  
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP7");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP9.class);
	    
	    job.setReducerClass(RSJReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(TaggedValue.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperCities.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperRegion.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return job.waitForCompletion(true)?0:1;
	}
  
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new ReduceSideJoin(), args));
	}
	
}
