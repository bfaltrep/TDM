import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Kmeans extends Configured implements Tool {

	public SortedSet<Point2D.Double> _pivots;
	
	public static class PointValue implements Writable{

		private Point2D.Double _point;
		private Point2D.Double _pivot;
		
		public  PointValue() {}
		
		public PointValue( Point2D.Double point, Point2D.Double pivot){
			this._point=(Point2D.Double)point.clone();
			this._pivot=(Point2D.Double)pivot.clone();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeDouble(_point.x);
			out.writeDouble(_point.y);
			
			out.writeDouble(_pivot.x);
			out.writeDouble(_pivot.y);
		}

		public void readFields(DataInput in) throws IOException {
			_point = new Point2D.Double();
			_point.x = in.readDouble();
			_point.y = in.readDouble();
			
			_pivot = new Point2D.Double();
			_pivot.x = in.readDouble();
			_pivot.y = in.readDouble();
		}
		
		public Point2D.Double getPoint(){
			return _point;
		}
		
		public Point2D.Double getMyContent(){
			return _pivot;
		}
	}
	
	
	
	public static class MapperCities extends Mapper<Object, Text, Text, PointValue>{
		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  String[] val = value.toString().split(",");
			  	context.write(new Text(val[0].toUpperCase()+","+val[3]),new PointValue(true,val[1]));
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
	
	private void initPivots(int nb_nodes){
	
		
	    /* generate initial nodes */
	    
	    _pivots = new TreeSet<Point2D.Double>();
	    while(_pivots.size()<nb_nodes)
	    	_pivots.add(Math.random());
	}
	
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    
	    conf.set("nb_nodes",args[2]);
	    
	    
	    Job job = Job.getInstance(conf, "TP9");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP9.class);
	    
	    job.setReducerClass(RSJReducer.class);
	    job.setOutputKeyClass(Text.class);
	    //job.setOutputValueClass(TaggedValue.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return job.waitForCompletion(true)?0:1;
	}
  
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Kmeans(), args));
	}
}
