package partII;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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

import Main.TP9;

public class KMeansND extends Configured implements Tool {

	public static class AverageWritable implements Writable{
		private Double _sum;
		private long _size;
		private List<Double> _cities;

		public  AverageWritable() {
			_cities = new Vector<Double>();
		}

		public AverageWritable(Double average, long size, List<Double> cities){
			_sum = average;
			_size = size;
			_cities = cities;
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(_sum);
			out.writeLong(_size);
			for (int i = 0; i < _cities.size(); i++) {
				out.writeDouble(_cities.get(i));
			}
		}

		public void readFields(DataInput in) throws IOException {
			_sum = in.readDouble();
			_size = in.readLong();
			for (int i = 0; i < _size; i++) {
				_cities.add(in.readDouble());
			}
		}

		public Double getSum(){
			return _sum;
		}

		public long getSize(){
			return _size;
		}

		public List<Double> getCityValue(){
			return _cities;
		}
	}

	/*/\ utiliser des sequences files*/

	/* Utils */

	private static double getClosestValue(Double pt, Set<Double> pivots){
		Iterator<Double> it = pivots.iterator();
		Double tmp = it.next();
		double dist = Math.abs(pt-tmp);
		double pivot = tmp;
		while(it.hasNext()){
			tmp = it.next();
			if(Math.abs(pt-tmp) < dist){
				dist = Math.abs(pt-tmp);
				pivot = tmp;
			}	
		}
		return pivot;
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
	 * map : pour chaque ligne, cherche le pivot le plus proche et écrit <pivot,ville>
	 * clean up : 
	 */
	public static class MapperKMeans extends Mapper<Object, Text, DoubleWritable, AverageWritable>{
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

			try{
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
					double closest = getClosestValue(val, _pivots);

					List<Double> tmp = new Vector<Double>(); 
					tmp.add(val);

					context.write(new DoubleWritable(closest),new AverageWritable(val,1,tmp));
				}

			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	// ----- COMBINER

	/*
	 * pour chaque clef, cherche la nouvelle ville la plus "centrale" localement. écrit <pivot, <moyenne locale, liste villes>>
	 */
	public static class CombinerKMeans extends Reducer<DoubleWritable,AverageWritable,DoubleWritable,AverageWritable> {

		public void reduce(DoubleWritable key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {
			//réduit ouesh

			long size = 0;
			double sum = 0;
			List<Double> cities = new Vector<Double>();

			for(AverageWritable city : values){
				sum += city.getSum();
				size += 1;
				cities.add(city.getCityValue().get(0));
			}
			context.write(key,new AverageWritable(sum, size, cities));
		}
	}

	// ----- REDUCER

	/*
	 * pour chaque clef (pivot), faire une moyenne totale puis chercher la ville la plus proche. écrit pour chaque ville <null,ville+","+newpivot>
	 */
	public static class ReducerKMeans extends Reducer<DoubleWritable,AverageWritable,NullWritable,Text> {

		public void reduce(DoubleWritable key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {

			double average = 0;
			long size = 0;
			double pivot;
			Set<Double> cities = new HashSet<Double>();

			for(AverageWritable p : values){
				size += p.getSize();
				average += p.getSum();

				//chaque combiner a réunit les valeurs des villes traitées pour cette clé dans une liste.
				cities.addAll(p.getCityValue());

			}

			//créer le nouveau pivot de l'ensemble en cherchant la ville la plus proche du pivot "calculé"/moyen.
			average/=size;
			pivot = getClosestValue(average, cities);
			context.write(NullWritable.get(),new Text(String.valueOf(pivot)));
		}
	}

	/* Runner Part */

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
				Double val1 = Double.parseDouble(br1.readLine());
				Double val2 = Double.parseDouble(br2.readLine());
				if(Math.abs(val1-val2) > 0.1){ res = false; break;}
			}

			//close
			br1.close();
			br2.close();
			input_fs1.close();
			input_fs2.close();
		}catch (Exception e){e.printStackTrace();}
		return res;
	}

	//args : inputfile  output(unused here)  nb_node [nombre variable de column_asked ] path_pivots  path_pivots_previous
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// -----  CONFIGURATION
		String input_file = args[0];
		int nb_node = 4; //par defaut
		try{
			nb_node = Integer.parseInt(args[2]);
			conf.setInt("nb_node", nb_node);
			//liste des colonnes traitées. 5 est le nombre d'arguments autres que les colonnes.
			conf.setInt("columns_nb",args.length-5);
			for (int i = 3; i < args.length-2; i++) {
				conf.setInt("column"+(i-3),Integer.parseInt(args[i]));
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		String path_pivots = args[args.length-1];
		String path_pivots_previous = args[args.length-2];

		Job job = Job.getInstance(conf, "Projet-kmeans-ND");

		job.addCacheFile(new Path(path_pivots_previous+"/part-r-00000").toUri());
		//job.setNumReduceTasks(1);
		job.setJarByClass(TP9.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperKMeans.class);
		job.setReducerClass(ReducerKMeans.class);
		job.setCombinerClass(CombinerKMeans.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(AverageWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input_file));
		FileOutputFormat.setOutputPath(job, new Path(path_pivots)); 

		System.out.println("\033[0;34m MapReduce : \033[0m "+job.waitForCompletion(true));

		// compare pivots with previous pivots : 0 = differents. 1 = same.
		return comparePivots(path_pivots+"/part-r-00000", path_pivots_previous+"/part-r-00000", nb_node, conf)?1:0;


	}
}
