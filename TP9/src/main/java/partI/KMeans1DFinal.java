package partI;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import Main.TP9;

public class KMeans1DFinal extends Configured implements Tool {

	/* Utils */

	private static int getClosestIndex(Double pt, Set<Double> pivots){
		Iterator<Double> it = pivots.iterator();
		Double tmp = it.next();
		double dist = Math.abs(pt-tmp);
		int i = 0,index = 0;

		while(it.hasNext()){
			tmp = it.next();
			i++;
			if(Math.abs(pt-tmp) < dist){
				dist = Math.abs(pt-tmp);
				index = i;
			}

		}
		return index;
	}

	private static Set<Double> readCachedFile(URI path_str){
		Set<Double> pivots = new HashSet<Double>();
		try {
			BufferedReader br = new BufferedReader( new FileReader(new File (path_str.getPath()).getName()));
			String pattern;
			while ((pattern = br.readLine()) != null) {
				pivots.add(Double.parseDouble(pattern));
			}
			br.close();
		}	catch (EOFException exc) {}
		catch(Exception exc){exc.printStackTrace();}
		return pivots;
	}

	/* MapReduce Part */

	// ----- MAPPER

	/*
	 * set up : recup la liste des pivots du fichier cache
	 * map : pour chaque ligne, cherche le pivot le plus proche et rajoute son id (cluster) à la fin de la ligne
	 * clean up : 
	 */
	public static class MapperKMeansFinal extends Mapper<Object,Text,Text,Text>{
		private Set<Double> _pivots;
		private int _asked;

		protected void setup(Context context){
			try {
				_asked = context.getConfiguration().getInt("asked",-1);
				URI[] files = context.getCacheFiles();
				_pivots = readCachedFile(files[0]);
			}	catch (IOException e) {e.printStackTrace();}		
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] val = value.toString().split(",");
			context.getConfiguration().get("asked");
			try
			{
				// si la ville est valide => si la colonne demandée a du contenu
				if(!val[_asked].matches("")){
					Integer closest = getClosestIndex(Double.parseDouble(val[_asked]), _pivots);
					//nous utilisons value comme clé pour l'obliger à conserver l'ordre des lignes.
					context.write(value,new Text(closest.toString()));
				}
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	// ----- REDUCER

	public static class ReducerKMeansFinal extends Reducer<Text,Text,NullWritable,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text t : values)
				context.write(NullWritable.get(),new Text(key.toString()+","+t.toString()));
		}
	}

	/* Runner Part */

	//args : inputfile  output  nb_node  column_asked path_pivots output_directory
	public int run(String[] args) throws Exception {
		System.out.println("\033[0;34m FINAL \033[0m"); //TMP
		Configuration conf = new Configuration();

		//recup args.
		String input_file = args[0];
		int nb_node = Integer.parseInt(args[2]);
		String column_asked = args[3];
		String path_pivots = args[4];

		conf.setInt("nb_node", nb_node);
		conf.set("asked", column_asked);

		Job job = Job.getInstance(conf, "Projet-kmeans-1D");

		job.addCacheFile(new Path(path_pivots+"/part-r-00000").toUri());

		job.setNumReduceTasks(1);
		job.setJarByClass(TP9.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperKMeansFinal.class);
		job.setReducerClass(ReducerKMeansFinal.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input_file));
		FileOutputFormat.setOutputPath(job, new Path(new URI(args[5]).normalize())); 

		return job.waitForCompletion(false)?0:1;
	}
}
