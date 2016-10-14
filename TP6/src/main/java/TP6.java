import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TP6 extends Configured implements Tool {

	public static class MyComparable implements Comparable{
		private long _pop;
		private String _town;

		public MyComparable(long pop, String town){
			_pop = pop;
			_town = town;
		}

		public int compareTo(Object o) {

			if(o instanceof MyComparable){
				MyComparable thing = ((MyComparable)o);
				if(_pop > thing.getPop())
					return 1;
				if(_pop < thing.getPop())
					return -1;
				return _town.compareTo(thing.getTown());
			}
			return 0;
		}

		public long getPop(){
			return _pop;
		}

		public String getTown(){
			return _town;
		}

	}


	public static class TP6Mapper extends Mapper<Object, Text, LongWritable, Text>{
		public int size_top;
		private TreeMap<MyComparable,String> top;
		private Text value;
		private LongWritable key;

		public void setup(Context context){
			size_top = context.getConfiguration().getInt("stop", 10);
			top = new TreeMap<MyComparable,String>();
			value = new Text();
			key = new LongWritable();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			try
			{
				if (tokens.length < 7 || tokens[4].length()==0 ) return;
				if(!tokens[4].matches("") && !tokens[1].matches(""))
				{
					long pop = Long.parseLong(tokens[4]);
					if(pop < 1) return;

					top.put(new MyComparable(pop, new String(tokens[1])), new String(tokens[1]));

					if (top.size() > size_top)
						top.remove(top.firstKey());
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

			for (Entry<MyComparable,String> entry : top.entrySet())
			{
				key.set(entry.getKey().getPop());
				value.set(entry.getValue());
				context.write(key, value);
			}
		}
	}

	public static class TP6Combiner extends Reducer<LongWritable, Text,LongWritable, Text>{
		public int size_top;
		private TreeMap<MyComparable,String> top;
		private Text value;
		private LongWritable key;

		public void setup(Context context){
			size_top = context.getConfiguration().getInt("stop", 10);
			top = new TreeMap<MyComparable,String>();
			value = new Text();
			key = new LongWritable();
		}

		public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

			for(Text val : value)
			{
				top.put(new MyComparable(key.get(), val.toString()), val.toString());

				if (top.size() > size_top)
					top.remove(top.firstKey());
			}

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

			for (Entry<MyComparable,String> entry : top.entrySet())
			{
				key.set(entry.getKey().getPop());
				value.set(entry.getValue());
				context.write(key, value);
			}
		}


	}

	public static class TP6Reducer extends Reducer<LongWritable,Text,IntWritable,Text> {
		public int size_top;
		private TreeMap<MyComparable,String> top;
		private Text _value;
		private IntWritable _key;

		public void setup(Context context){
			size_top = context.getConfiguration().getInt("stop", 10);
			top = new TreeMap<MyComparable,String>();
			_value = new Text();
			_key = new IntWritable();
		}

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val : values)
			{
				top.put(new MyComparable(key.get(), val.toString()), val.toString());

				if (top.size() > size_top)
					top.remove(top.firstKey());
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			NavigableMap<MyComparable, String> returned = top.descendingMap();

			int i = 0;
			for (Entry<MyComparable,String> entry : returned.entrySet())
			{
				i++;
				_key.set(i);
				_value.set(entry.getValue()+" "+entry.getKey().getPop());
				context.write(_key, _value);
			}
		}
	}

	public void usage() {
		System.out.println("Invalid arguments, waiting for 3 parameters : file_input directory_output <int>size_top.");
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		/*
		int size_top;
		try {
			size_top = Integer.parseInt(args[2]);
		}
		catch(Exception e) {
			usage();
			return -1;
		}*/

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "TP6");
		job.setNumReduceTasks(1);
		conf.setInt("stop", Integer.parseInt(args[2]));
		job.setJarByClass(TP6.class);


		job.setMapperClass(TP6Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(TP6Combiner.class);

		job.setReducerClass(TP6Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TP6(), args);

		try{
			URI uri = new URI(args[1]+"/part-r-00000");
			uri=uri.normalize();
			FileSystem file = FileSystem.get(uri, new Configuration(), "bfaltrep");
			Path path = new Path(uri.getPath());
			BufferedReader buffer = new BufferedReader(new InputStreamReader(file.open(path)));

			String line;
			line = buffer.readLine();
			while (line!=null) {
				System.out.println(line);
				line = buffer.readLine();   
			}

			URI uri_rep = new URI(args[1]);
			uri_rep = uri_rep.normalize();
			FileSystem _rep = FileSystem.get(uri_rep, new Configuration(), "bfaltrep");
			Path path_rep = new Path(uri_rep.getPath());

			_rep.delete(path_rep, true);

		}
		catch(Exception e){e.printStackTrace();}	

		System.exit(res);


	}
}
