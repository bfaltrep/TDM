import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TP5V2 {
	public static class TP5Mapper extends Mapper<LongWritable, Point2DWritable, LongWritable, Point2DWritable>{

		public void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
			context.write(key,value);
		}
	}
	public static class TP5Reducer extends Reducer<LongWritable, Point2DWritable, LongWritable, Text> {
		
		private Text _res = new Text();
		
		public void reduce(LongWritable key, Iterable<Point2DWritable> values, Context context) throws IOException, InterruptedException {
			
			StringBuffer res = new StringBuffer();
			for(Point2DWritable point : values)
				res.append(point.getPoint().toString()+"\n");
			//res.deleteCharAt(res.length()-1);
			
			_res.set(res.toString());
			context.write(key,_res);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// -- used only by our InputFormat.
		conf.set("nb_mapper",args[1]);
		conf.set("point_by_split",args[2]);

		Job job = Job.getInstance(conf, "TP5V2");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP5V2.class);
		job.setMapperClass(TP5Mapper.class);
		// TODO : adapter
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Point2DWritable.class);

		//job.setCombinerClass(TP5Combiner.class);
		job.setReducerClass(TP5Reducer.class);
		// TODO : adapter
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setInputFormatClass(RandomPointInputFormat.class);

		
		//FileInputFormat.addInputPath(job, null);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		boolean res = job.waitForCompletion(true);
		System.exit( res ? 0 : 1);
	}
}
