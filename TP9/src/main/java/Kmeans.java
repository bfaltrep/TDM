import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* http://blog.data-miners.com/2008_02_01_archive.html */

public class Kmeans extends Configured implements Tool {

	/* Classes */
	
	public static class PointWritable implements Writable{
		private Point2D.Double _point;
		
		public  PointWritable() {}
		
		public PointWritable( Point2D.Double point){
			this._point=(Point2D.Double)point.clone();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeDouble(_point.x);
			out.writeDouble(_point.y);
		}

		public void readFields(DataInput in) throws IOException {
			_point = new Point2D.Double();
			_point.x = in.readDouble();
			_point.y = in.readDouble();
		}
		
		public Point2D.Double getPoint(){
			return _point;
		}
	}
	
	public static class AverageWritable implements Writable{
		private Point2D.Double _add;
		private long _size;
		
		public  AverageWritable() {}
		
		public AverageWritable(Point2D.Double average, long size){
			_add = average;
			_size = size;
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeDouble(_add.x);
			out.writeDouble(_add.y);
			out.writeLong(_size);
		}

		public void readFields(DataInput in) throws IOException {
			_add = new Point2D.Double();
			_add.x = in.readDouble();
			_add.y = in.readDouble();
			_size = in.readLong();
		}
		
		public Point2D.Double getAdded(){
			return _add;
		}
		
		public long getSize(){
			return _size;
		}
	}
	
	/* Methods */
	
	private static Set<Point2D.Double> readCachedFile(String path){
		java.io.File file = new java.io.File(path);
	    java.io.FileInputStream filestream = null;
	    java.io.ObjectInputStream objectstream = null;
	    Set<Point2D.Double> _pivots = new HashSet<Point2D.Double>();

	    try {
			
			filestream = new FileInputStream(file);
			objectstream = new ObjectInputStream(filestream);
			
			while(_pivots.size() != -1)
				_pivots.add((Point2D.Double) (objectstream.readObject()));
			
			objectstream.close();
			filestream.close();
    	
		} catch (EOFException exc) {}
	    catch(Exception exc){
	    	exc.printStackTrace();
	    }
	   
	   return _pivots;
	}
	
	private static int getClosest(Point2D.Double pt, Set<Point2D.Double> pivots){
		Iterator<Point2D.Double> it = pivots.iterator();
		Point2D.Double tmp = it.next();
		int closest = 0;
		double dist = pt.distance(tmp);
		int i = 0;
		while(it.hasNext()){
			tmp = it.next();
			if(dist > pt.distance(tmp)){
				closest = i;
				dist = pt.distance(tmp);
			}
			i++;
		}
		return closest;
	}
	
	/* MapReduce Part */
	
	public static class MapperKMeans extends Mapper<Object, Text, IntWritable, PointWritable>{
		private Set<Point2D.Double> _pivots;
		
		protected void setup(Context context){
			try {
				URI[] files = context.getCacheFiles();
				_pivots = readCachedFile(files[0].getPath());
			} catch (IOException e) {e.printStackTrace();}		
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			  String[] val = value.toString().split(",");
			  Point2D.Double pt = new Point2D.Double(new BigDecimal(val[0]).doubleValue(),new BigDecimal(val[1]).doubleValue());
			  int closest = getClosest(pt, _pivots);
			  	context.write(new IntWritable(closest),new PointWritable(pt));
			  }
		  }
	
	public static class CombinerKMeans extends Reducer<IntWritable,PointWritable,IntWritable,AverageWritable> {
		private Set<Point2D.Double> _pivots;
		
		protected void setup(Context context){
			try {
				URI[] files = context.getCacheFiles();
				_pivots = readCachedFile(files[0].getPath());
			} catch (IOException e) {e.printStackTrace();}
		}
		
		public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
			//reduit ouesh
			int size = 0;
			Point2D.Double add = new Point2D.Double(0,0);
			for(PointWritable p : values){
				add.x += p.getPoint().getX();
				add.y += p.getPoint().getY();
				size++;
			}
				
			context.write(key,new AverageWritable(add, size));
		}
	}
	
	public static class ReducerKMeans extends Reducer<IntWritable,AverageWritable,Text,Text> {
	    public void reduce(IntWritable key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {
	    	
	    	Point2D.Double average = new Point2D.Double(0,0);
	    	
	    	long size = 0;
			for(AverageWritable p : values){
				size += p.getSize();
				average.x += p.getAdded().getX();
				average.y += p.getAdded().getY();
			}

			average.x/=size;
			average.y/=size;
			
	    	context.write(new Text("Nb Iterations"),new Text(average.x+","+average.y));
	    }
	    
	  }
	
	private double generatePoint(){
		double val = Math.random()*100;
		if(Math.random() < 0.5)
			val *= -1;
		return val;
	}
	
	private String initPivots(int nb_nodes){
		
		/* write them into a tmp file*/
		
	    String path = "/tmp/"+"kmeans"+(int)(Math.random()*100)+".txt";
	    java.io.File file = new java.io.File(path);
	    java.io.FileOutputStream filestream = null;
	    java.io.ObjectOutputStream objectstream = null;
	    try
	    {
		    file.createNewFile();
		    filestream = new java.io.FileOutputStream(file);
		    objectstream = new ObjectOutputStream(filestream);
		    
		    for (int i = 0; i < nb_nodes; i++) {
				objectstream.writeObject(new Point2D.Double(generatePoint(),generatePoint()));
			}
		    
		    objectstream.close();
		    filestream.close();
		}catch (Exception e){e.printStackTrace();}
	    return path;
	}
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    
	    String path_pivots = initPivots(Integer.parseInt(args[2]));
	    conf.set("nb_nodes",args[2]);
	    
	    Job job = Job.getInstance(conf, "TP9");
	    
	    try {
			job.addCacheFile(new URI(path_pivots));
		} catch (URISyntaxException e){e.printStackTrace();}
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP9.class);
	    
	    job.setMapperClass(MapperKMeans.class);
	    job.setReducerClass(ReducerKMeans.class);
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
		System.exit(ToolRunner.run(new Kmeans(), args)); //TODO: a retirer
		/* TODO:
		 * pour boucler : while sur run ? 
		 * 
		 * si on boucle, doit faire varier le fichier de sortie/ entrée de pivot en fonction de l'appel dans main.
		*/
	}
}
