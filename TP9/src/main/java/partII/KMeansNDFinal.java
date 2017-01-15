package partII;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class KMeansNDFinal extends Configured implements Tool {

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
		private List<Integer> _columns;

		protected void setup(Context context){
			try {
				int column_nb = context.getConfiguration().getInt("columns_nb",1);
				_columns = new ArrayList<Integer>(column_nb);
				for (int i = 0; i < column_nb; i++) {
					_columns.add(context.getConfiguration().getInt("column"+i,2));
				}

				URI[] files = context.getCacheFiles();
				_pivots = readCachedFile(files[0]);
			}	catch (IOException e) {e.printStackTrace();}		
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] blocs = value.toString().split(",");
			try
			{
				// si la ville est valide => si les colonnes demandées ont du contenu
				double val = 0;
				boolean control=true;
				for(Integer ask : _columns){
					if(blocs[ask].matches("")){
						control=false;
						break;
					}else{
						val += Double.parseDouble(blocs[ask]);
					}
				}
				if(control){
					Integer closest = getClosestIndex(val, _pivots);
					//nous utilisons value comme clé pour l'obliger à conserver l'ordre des lignes.
					context.write(value,new Text(closest.toString()));
				}else{
					context.write(value,new Text(""));
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
			for(Text t : values){
				//nous conservons les lignes non valides mais n'ajoutons pas de colonne finale.
				if(t.toString().matches(""))
					context.write(NullWritable.get(),new Text(key.toString()));
				else
					context.write(NullWritable.get(),new Text(key.toString()+","+t.toString()));
			}
				
		}
	}

	/* Runner Part */

	//args : inputfile  output_file(unused) nb_node [multiples column_asked] path_pivots output_directory
	public int run(String[] args) throws Exception {
		System.out.println("\033[0;34m FINAL \033[0m"); //TMP
		Configuration conf = new Configuration();

		//recup args.
		String input_file = args[0];
		int nb_node = Integer.parseInt(args[2]);
		//liste des colonnes traitées. 5 est le nombre d'arguments autres que les colonnes.
		conf.setInt("columns_nb",args.length-5);
		for (int i = 3; i < args.length-2; i++) {
			conf.setInt("column"+(i-3),Integer.parseInt(args[i]));
		}
		String path_pivots = args[args.length-2];

		conf.setInt("nb_node", nb_node);

		Job job = Job.getInstance(conf, "Projet-kmeans-ND");

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
		FileOutputFormat.setOutputPath(job, new Path(new URI(args[args.length-1]).normalize())); 

		return job.waitForCompletion(false)?0:1;
	}
}
