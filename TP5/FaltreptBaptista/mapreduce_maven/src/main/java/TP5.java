import java.awt.geom.Point2D;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {
	public static boolean pi_quart(Point2D.Double pt){
		double dist = Math.pow(pt.getX(),2)+Math.pow(pt.getY(),2);
		return dist <= 1;
	}
	
	public static class TP5Mapper extends Mapper<LongWritable, Point2DWritable, LongWritable, Point2DWritable>{

		public void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
			context.write(key,value);
		}
	}

	public static class TP5Combiner extends Reducer<LongWritable, Point2DWritable, LongWritable, Point2DWritable>{
		
		private long _nb_circle;
		private long _nb_total;
		
		protected void setup(Context context)
		{
			
			_nb_circle = 0;
			_nb_total = 0;
		}
		
		public void reduce(LongWritable key, Iterable<Point2DWritable> values, Context context) throws IOException, InterruptedException {
			for(Point2DWritable point : values)
			{
				_nb_total++;
				if(pi_quart(point.getPoint()))
					_nb_circle++;
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{

			context.write(new LongWritable(_nb_total),new Point2DWritable((double)_nb_total, (double)_nb_circle));
		}
	}
	
	public static class TP5Reducer extends Reducer<LongWritable, Point2DWritable, NullWritable, Text> {
		
		private Text _val = new Text();
		private NullWritable _key;
		
		private long _nb_circle;
		private long _nb_total;

		protected void setup(Context context)
		{
			_nb_circle = 0;
			_nb_total = 0;
		}
		
		public void reduce(LongWritable key, Iterable<Point2DWritable> values, Context context) throws IOException, InterruptedException {
			
			for(Point2DWritable input : values)
			{
				_nb_circle+=(long)input.getPoint().getY();
				_nb_total+=(long)input.getPoint().getX();
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			BigDecimal res = new BigDecimal(4.0);
			res = res.multiply(new BigDecimal(_nb_circle));
			res = res.divide(new BigDecimal(_nb_total));
			_val.set("\nPi approximation "+res.doubleValue()+"\n"+_nb_total);
			context.write(_key,_val);
		}
	}
	
	/* Main */
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("nb_mapper",Integer.parseInt(args[1]));
		conf.setLong("point_by_split",Long.parseLong(args[2]));

		Job job = Job.getInstance(conf, "TP5");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP5.class);
		job.setMapperClass(TP5Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Point2DWritable.class);
		
		job.setCombinerClass(TP5Combiner.class);
		job.setReducerClass(TP5Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(RandomPointInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		System.exit( job.waitForCompletion(true) ? 0 : 1);
	}
}
