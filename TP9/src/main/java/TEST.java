import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class TEST extends Configured implements Tool {
	
	public static class MapperKMeans extends Mapper<Object, Text, LongWritable, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split(",");

			//if(val[4].matches("Population"))
			{
				StringBuffer tmp = new StringBuffer();

				tmp.append(val[0]+"\n");
				context.write(new LongWritable(1),new Text(tmp.toString()));
			}
		}
	}

	// ----- REDUCER

	public static class ReducerKMeans extends Reducer<LongWritable,Text,Text,Text> {
	
	    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	StringBuffer tmp = new StringBuffer();

	    	for(Text v : values)
	    		tmp.append(v.toString()+"\n");
	    	
			context.write(new Text("1"),new Text(tmp.toString()));
	    }
	  }
	
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
	
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf, "TEST");
	
		String input_file = args[0];
		int nb_node = Integer.parseInt(args[2]);
		String column_asked = args[3];
		String path_pivots_previous = args[6];
		
		initPivots(input_file, path_pivots_previous+"/part-r-00000",nb_node, Integer.parseInt(column_asked), conf);
		
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
	    FileOutputFormat.setOutputPath(job, new Path(args[5])); 
	   
	    if (job.waitForCompletion(true))
	    	System.out.println("C EST FINIT.");
	    else
		    System.out.println("PROBLEME : CA DEVRAIT ETRE FINIT.");
		return 0;
	    
	}
}
