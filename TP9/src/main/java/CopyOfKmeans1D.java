
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class CopyOfKmeans1D extends Configured implements Tool {
	 private static final Logger LOG = Logger.getLogger(CopyOfKmeans1D.class); //TMP

	/* MapReduce Part */
	
	// ----- MAPPER
	
	/*
	 * set up : recup la liste des pivots du fichier cache
	 * map : pour chaque ligne, cherche le pivot le plus proche et écrit <pivot,ville>
	 * clean up : 
	*/
	public static class MapperKMeans extends Mapper<Object, Text, LongWritable, Text>{
		private Set<Double> _pivots;
		private int _asked;
		
		//recupération des données du fichier de cache (pivots) => un set de double
		private void readCachedFile(URI path_str){
			
		    _pivots = new HashSet<Double>();
		    try {
				BufferedReader br = new BufferedReader( new FileReader(new File(path_str.getPath()).getName()));
				String pattern;
			    while ((pattern = br.readLine()) != null) {
			      _pivots.add(Double.parseDouble(pattern.split(",")[_asked]));
			    }

			    br.close();
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
			LOG.info("LOG DANS LE MAPPER");

			String[] val = value.toString().split(",");
			context.getConfiguration().get("asked");

			
			if(val[_asked].matches("Population")){
				StringBuffer tmp = new StringBuffer();
				tmp.append("TEST1");
				for(Double d : _pivots)
					tmp.append(d.toString()+"\n");
				context.write(new LongWritable(1),new Text(tmp.toString()));
			}
		}
	}

	// ----- REDUCER
	
	/*
	 * pour chaque clef (pivot), faire une moyenne totale puis chercher la ville la plus proche. écrit pour chaque ville <null,ville+","+newpivot>
	*/
	public static class ReducerKMeans extends Reducer<LongWritable,Text,Text,Text> {
	
	    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	StringBuffer tmp = new StringBuffer();
	    	tmp.append("TEST2");
	    	for(Text v : values)
	    		tmp.append(v.toString()+"\n");
	    	
			context.write(new Text("1"),new Text(tmp.toString()));
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
	
	//args : inputfile  output  nb_node  column_asked  path_pivots  path_pivots_previous
	public int run(String[] args) throws Exception {
		System.out.println(" pivot "+args[5]+" // previous pivot "+args[6]); //TMP
		Configuration conf = new Configuration();
		String path_pivots = args[args.length-2];
		String path_pivots_previous = args[args.length-1];
		
		int nb_node = Integer.parseInt(args[2]);
		conf.setInt("nb_node", nb_node);
		conf.set("asked", args[3]);

		Job job = Job.getInstance(conf, "Projet-kmeans-test-cachedfile");

		if(Integer.parseInt(args[4]) == 0)
			initPivots(args[0], path_pivots_previous,nb_node, Integer.parseInt(args[3]), conf);
					
		job.addCacheFile(new Path(path_pivots_previous).toUri());
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
	    FileOutputFormat.setOutputPath(job, new Path(path_pivots)); 
	   
	    if (job.waitForCompletion(true))
	    	System.out.println("C EST FINIT.");
	    else
		    System.out.println("PROBLEME : CA DEVRAIT ETRE FINIT.");
		return 0;
	    
	}
}
