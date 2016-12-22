
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;

public class Kmeans1D extends Configured implements Tool {
	

	public static class AverageWritable implements Writable{
		private Double _sum;
		private long _size;
		private List<String> _cities;
		
		public  AverageWritable() {
			_cities = new Vector<String>();
		}
		
		public AverageWritable(Double average, long size, List<String> cities){
			_sum = average;
			_size = size;
			_cities = cities;
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeDouble(_sum);
			out.writeLong(_size);
			for (int i = 0; i < _cities.size(); i++) {
				out.writeUTF(_cities.get(i));
			}
		}

		public void readFields(DataInput in) throws IOException {
			_sum = in.readDouble();
			_size = in.readLong();
			for (int i = 0; i < _size; i++) {
				_cities.add(in.readUTF());
			}
		}
		
		public Double getSum(){
			return _sum;
		}
		
		public long getSize(){
			return _size;
		}
		
		public List<String> getCityLine(){
			return _cities;
		}
	}
	
	/*/\ utiliser des sequences files*/
	
	
	/* Utils */

	private static int getClosest(Double pt, Set<Double> pivots){
		Iterator<Double> it = pivots.iterator();
		Double tmp = it.next();
		int closest = 0;
		double dist = Math.abs(pt-tmp);
		int i = 0;
		while(it.hasNext()){
			tmp = it.next();
			if(dist > Math.abs(pt-tmp)){
				closest = i;
				dist = Math.abs(pt-tmp);
			}
			i++;
		}
		return closest;
	}
		
	/* MapReduce Part */
	
	// ----- MAPPER
	
	/*
	 * set up : recup la liste des pivots du fichier cache
	 * map : pour chaque ligne, cherche le pivot le plus proche et écrit <pivot,ville>
	 * clean up : 
	*/
	public static class MapperKMeans extends Mapper<Object, Text, LongWritable, AverageWritable>{
		private Set<Double> _pivots;
		private int _asked;
				
		private void readCachedFile(URI path_str){
		    _pivots = new HashSet<Double>();
		    try {
		    	BufferedReader br = new BufferedReader( new FileReader(new File (path_str.getPath()).getName()));
				String pattern;
			    while ((pattern = br.readLine()) != null) {
			      _pivots.add(Double.parseDouble(pattern.split(",")[_asked]));
				}
			} catch (EOFException exc) {}
		    catch(Exception exc){
		    	exc.printStackTrace();
		    }
		}
		
		protected void setup(Context context){
			try {
				_asked = context.getConfiguration().getInt("asked",-1);
				URI[] files = context.getCacheFiles();
				readCachedFile(files[0]);
			} catch (IOException e) {e.printStackTrace();}		
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			  String[] val = value.toString().split(",");
			  context.getConfiguration().get("asked");
			  
			  if(!val[_asked].matches("") && !val[_asked].matches("Population")){
				  int closest = getClosest(Double.parseDouble(val[_asked]), _pivots);
				  List<String> tmp = new Vector<String>(); tmp.add(value.toString());
				  context.write(new LongWritable(closest),new AverageWritable(Double.parseDouble(val[_asked]),1,tmp));
			  }
			
			
		}
	}
	
	// ----- COMBINER
	
	/*
	 * pour chaque clef, cherche la nouvelle ville la plus "centrale" localement. écrit <pivot, <moyenne locale, liste villes>>
	*/
	public static class CombinerKMeans extends Reducer<LongWritable,AverageWritable,LongWritable,AverageWritable> {
		private Set<Double> _pivots;
		private int _asked;
		
		private Set<Double> readCachedFile(String path_str, Context context){
		    Set<Double> _pivots = new HashSet<Double>();
		    try {
				
				URI uri = new URI(path_str).normalize();
				FileSystem fs = FileSystem.get(uri, context.getConfiguration(), "bfaltrep");
				Path path = new Path(uri.getPath());
				InputStream is = fs.open(path);
				BufferedReader br = new BufferedReader( new InputStreamReader( is, "UTF-8"));
				
				while(br.ready())
					_pivots.add(Double.parseDouble(br.readLine().split(",")[_asked]));
			} catch (EOFException exc) {}
		    catch(Exception exc){
		    	exc.printStackTrace();
		    }
		   return _pivots;
		}

		protected void setup(Context context){
			try {
				_asked = context.getConfiguration().getInt("asked",-1);
				URI[] files = context.getCacheFiles();
				
				_pivots = readCachedFile(files[0].getPath(), context);
			} catch (IOException e) {e.printStackTrace();}		
		}
		
		public void reduce(LongWritable key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {
			//reduit ouesh
			long size = 0;
			double sum = 0;
			List<String> cities = new Vector<String>();
			
			for(AverageWritable citie : values){
				sum += Double.parseDouble(citie.getCityLine().get(0).toString().split(",")[_asked]);
				size++;
				cities.add(citie.getCityLine().get(0).toString());
			}
			context.write(key,new AverageWritable(sum, size, cities));
		}
	}
	
	// ----- REDUCER
	
	/*
	 * pour chaque clef (pivot), faire une moyenne totale puis chercher la ville la plus proche. écrit pour chaque ville <null,ville+","+newpivot>
	*/
	public static class ReducerKMeans extends Reducer<LongWritable,AverageWritable,Text,Text> {
		private int _asked;
		private long _iteration;
		
		
		protected void setup(Context context){
			_asked = context.getConfiguration().getInt("asked",-1);	
			_iteration = context.getConfiguration().getInt("nbIteration",-1);	
		}
	
	    public void reduce(LongWritable key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {
	    	
	    	double average = 0;
	    	long size = 0;
	    	long pivot;
	    	Set<Double> cities = new HashSet<Double>();
	    	
			for(AverageWritable p : values){
				size += p.getSize();
				average += p.getSum();
				
				List<String> citieslines = p.getCityLine();
				for (int i = 0; i < citieslines.size(); i++) {
					cities.add(Double.parseDouble(citieslines.get(i).split(",")[_asked]));
				}
			}

			average/=size;
			
			pivot = getClosest(average, cities);
			
			context.write(new Text(String.valueOf(_iteration)),new Text(String.valueOf(pivot)));
	    	//context.write(new LongWritable(_iteration),new LongWritable(pivot));
	    }
	  }
	
	
	
	
	
	
	/* Runner Part */
	
	/*
	 * recup les nb_nodes premières villes du fichier d'entrées et les placent dans un fichier ds HDFS dont on retourne le chemin.
	*/
	private void initPivots(String input, String output, int nb_node, int asked, Configuration conf){
		try
	    {
			// output file : create
			URI output_uri = new URI(output).normalize();
			FileSystem output_fs = FileSystem.get(output_uri, conf, "bfaltrep");
			Path output_path = new Path(output_uri.getPath());
			
			if (output_fs.exists(output_path)) { output_fs.delete(output_path, true); } 
			OutputStream os = output_fs.create(output_path, new Progressable(){public void progress(){}});
			
			// input file : open
			URI input_uri = new URI(input).normalize();
			FileSystem input_fs = FileSystem.get(input_uri, conf, "bfaltrep");
			Path input_path = new Path(input_uri.getPath());
			
			InputStream is = input_fs.open(input_path);

			//copy
			BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ));
			BufferedReader br = new BufferedReader( new InputStreamReader( is, "UTF-8"));
			br.readLine(); //retire la première ligne qui contient les intitulés de colonnes.
			int i = 0;
			while (i < nb_node) {
				String val = br.readLine();
				String[] tmp = val.split(",");
				if(!tmp[asked].matches("")){
					bw.write(val+"\n");
					i++;
				}
			}
			br.close();
			bw.close();
			input_fs.close();
			output_fs.close();
		}catch (Exception e){e.printStackTrace();}
	}
	
	private boolean comparePivots(String path, String path2, int nb_node, Configuration conf){
		boolean res = true;
		try
	    {
			//file 1
			URI input_uri1 = new URI(path).normalize();
			FileSystem input_fs1 = FileSystem.get(input_uri1, conf, "bfaltrep");
			Path input_path1 = new Path(input_uri1.getPath());
			
			InputStream is1 = input_fs1.open(input_path1);
			BufferedReader br1 = new BufferedReader( new InputStreamReader( is1, "UTF-8"));
			
			//file 2
			URI input_uri2 = new URI(path2).normalize();
			FileSystem input_fs2 = FileSystem.get(input_uri2, conf, "bfaltrep");
			Path input_path2 = new Path(input_uri2.getPath());
			
			InputStream is2 = input_fs2.open(input_path2);
			BufferedReader br2 = new BufferedReader( new InputStreamReader( is2, "UTF-8"));
			
			//compare
			for (int j = 0; j < nb_node; j++) {
				String val1 = br1.readLine();
				String val2 = br2.readLine();
				if (!val1.matches(val2)){ res = false; break;}
			}
			
			//close
			br1.close();
			br2.close();
			input_fs1.close();
			input_fs2.close();
	    }catch (Exception e){e.printStackTrace();}
		return res;
	}
	
	
	//args : inputfile  output  nb_node  column_asked nb_iteration path_pivots  path_pivots_previous
	public int run(String[] args) throws Exception {
		System.out.println(" pivot "+args[5]+" // previous pivot "+args[6]); //TMP
		Configuration conf = new Configuration();

		String input_file = args[0];
		String output_file = args[1];
		int nb_node = Integer.parseInt(args[2]);
		String column_asked = args[3];
		int nb_iteration = Integer.parseInt(args[4]);
		String path_pivots = args[5];
		String path_pivots_previous = args[6];
		
		
		if( nb_iteration == 0)
			initPivots(input_file, path_pivots_previous,nb_node, Integer.parseInt(column_asked), conf);
		
		conf.setInt("nb_node", nb_node);
		conf.set("asked", column_asked);
		conf.setInt("nbIteration", nb_iteration);
		
		Job job = Job.getInstance(conf, "Projet-kmeans-1D");
		
		job.addCacheFile(new Path(path_pivots_previous).toUri());
		
		job.setNumReduceTasks(1);
	    job.setJarByClass(TP9.class);
		
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setMapperClass(MapperKMeans.class);
	    job.setReducerClass(ReducerKMeans.class);
		job.setCombinerClass(CombinerKMeans.class);
	    
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(AverageWritable.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(input_file));
	    FileOutputFormat.setOutputPath(job, new Path(path_pivots)); 
	   
	    if (job.waitForCompletion(true)){
	    	System.out.println("CA MARCHE. :)");
	    	return comparePivots(path_pivots+"/part-r-00000", path_pivots_previous, nb_node, conf)?1:0;
	    }
	    else{
		    System.out.println("CA MARCHE PAS. :(");
		    // compare pivots with previous pivots : 0 = differents. 1 = same.
			return comparePivots(path_pivots+"/part-r-00000", path_pivots_previous, nb_node, conf)?1:0;
		}
	}
}
