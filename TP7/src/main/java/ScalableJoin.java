import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ReduceSideJoin.TaggedValue;

//notre cluster : yarn jar tp7-mapreduce-0.0.1.jar ScalableJoin /cities.txt /region.csv /tp7

public class ScalableJoin  extends Configured implements Tool {

	public static class TaggedKey implements WritableComparable{
		private String _country;
		private String _region;
		private boolean _side;
		 // Identifiant du jeu de données ??
		
		public TaggedKey(){}
		
		public TaggedKey(String country, String region, boolean side){
			_country = country;
			_region= region;
			_side = side;
		}
		
		public int compareTo(Object o) {
			if(o instanceof TaggedKey){
					int res = _country.compareTo(((TaggedKey)o).getCountry());
					if (res != 0)
						return res;
					res = _region.compareTo(((TaggedKey)o).getRegion());
					if (res != 0)
						return res;
					return _side == ((TaggedKey)o).getSide()?0:1;
			}
			return 0;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(_country);
			out.writeUTF(_region);
			out.writeBoolean(_side);
		}

		public void readFields(DataInput in) throws IOException {
			_country = in.readUTF();
			_region = in.readUTF();
			_side = in.readBoolean();
		}
		
		public String getCountry(){
			return _country;
		}
		
		public String getRegion(){
			return _region;
		}
		
		public boolean getSide(){
			return _side;
		}
		
		public int getNaturalKeyHashCode(){
			return _country.hashCode()+_region.hashCode();
		}
	}
	/*
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
	}*/
	
	public static class ValComparable implements WritableComparable{

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}}
	public static class Val2Comparable implements WritableComparable{

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}}
	
	  public static class MapperCities extends Mapper<Object, Text, TaggedKey, Text>{
		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  String[] val = value.toString().split(",");
			  if(!val[3].matches("Region")){
			  	  context.write(new TaggedKey(val[0].toUpperCase(),val[3],true),new Text(val[1]));
			  }
		  }
	  }
	  
	  public static class MapperRegion extends Mapper<Object, Text, TaggedKey, Text>{
		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  String[] val = value.toString().split(",");
			  context.write(new TaggedKey(val[0],val[1],false),new Text(val[2]));
		  }
	  }
	  
	public static class SJPartition extends Partitioner<TaggedKey, Text>{

		@Override
		public int getPartition(TaggedKey key, Text value, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			else
				return key.getNaturalKeyHashCode();
		}
	}
	
	public static class SJReducer extends Reducer<TaggedKey,Text,Text,Text> {
	    public void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	String region = "";
	    	
	    	for(Text val : values)
	    		if(!key.getSide()){
	    			region = val.toString();
	    			break;
	    		}

	    	for(Text val : values)
	    		if(key.getSide())
	    			context.write(new Text(region),new Text(val.toString()));
	    }
	    
	  }
	
	/*
	 * 
	Alice<tab>23<tab>female<tab>45
	Bob<tab>34<tab>male<tab>89        ----
	Kristine<tab>38<tab>female<tab>53 ----
	Connor<tab>25<tab>male<tab>27
	James<tab>34<tab>male<tab>79
	


	Nancy<tab>7<tab>female<tab>98     ----
	Adam<tab>9<tab>male<tab>37        ----
	Jacob<tab>7<tab>male<tab>23
	Mary<tab>6<tab>female<tab>93

	Alex<tab>52<tab>male<tab>69
	Chris<tab>67<tab>male<tab>97      ----
	Daniel<tab>78<tab>male<tab>95
	Clara<tab>87<tab>female<tab>72
	Monica<tab>56<tab>female<tab>92   ----
	 * 
	 * 
	 * 
	 * 
	*/
	public int run(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP7_ScalableJoin");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP7.class);
	    
	    job.setMapOutputKeyClass(TaggedKey.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setPartitionerClass(SJPartition.class);
	    job.setReducerClass(SJReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReduceSideJoin.MapperCities.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReduceSideJoin.MapperRegion.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new ScalableJoin(), args));
	}


	
}
