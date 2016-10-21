import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ScalableJoin  extends Configured implements Tool {

	public static class TaggedKey implements WritableComparable{
		private String _country;
		private String _region;
		private boolean _side;
		
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
					
					if(_side == ((TaggedKey)o).getSide())
						return 0;
					else if(_side)
						return 1;
					else 
						return -1;
			}
			return 0;
		}
		
		public int compareTo(TaggedKey tk) {
			return this.getNaturalKeyHashCode() - tk.getNaturalKeyHashCode();
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
	
	public static class JoinGrouping extends WritableComparator{
		 public JoinGrouping() {
	            // evite l'appel au constructeur par defaut qui provoque une NullPointerException
	            super(TaggedKey.class, true);
	        }
		 
		 public int compare (WritableComparable a, WritableComparable b){
			 return ((TaggedKey)a).compareTo((TaggedKey)b);
		 }
	}
	
	public static class JoinSort extends WritableComparator {
	        public JoinSort() {
	            super(TaggedKey.class, true);
	        }
	        
	        public int compare (WritableComparable a, WritableComparable b){
	        	return ((TaggedKey)a).compareTo(b);
			 }

	}
		
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
			  context.write(new TaggedKey(val[0].toUpperCase(),val[1],false),new Text(val[2]));
		  }
	  }
	  
	public static class SJPartition extends Partitioner<TaggedKey, Text>{

		@Override
		public int getPartition(TaggedKey key, Text value, int numPartitions) {
			if(numPartitions == 0)
				return 0;
			else
				return key.getNaturalKeyHashCode() % numPartitions;
		}
	}
	
	public static class SJReducer extends Reducer<TaggedKey,Text,Text,Text> {
	    public void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    	Iterator<Text> it = values.iterator();
	    	String line = it.next().toString();
	    	// Si nous avons bien une région associé à cet ensemble de villes.
	    	// puis si nous avons bien des villes associé à cette région.
	    	if(!key._side && it.hasNext()){
	    		String region = new String(line);
	    			while(it.hasNext())
	    				context.write(new Text(region),new Text(it.next().toString()));
	    	}
	    	
	    }
	  }
	
	public int run(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP7_ScalableJoin");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP7.class);
	    
	    job.setMapOutputKeyClass(TaggedKey.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setPartitionerClass(SJPartition.class);
	    job.setGroupingComparatorClass(JoinGrouping.class);
	    job.setSortComparatorClass(JoinSort.class);
	    job.setReducerClass(SJReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperCities.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperRegion.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new ScalableJoin(), args));
	}


	
}
