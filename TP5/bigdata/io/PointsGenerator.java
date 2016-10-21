/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférencces HDR
 * LaBRI: Université de Bordeaux
 */
package bigdata.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PointsGenerator extends Configured implements Tool {

	public static class IdentityMapper extends Mapper<LongWritable, Point2DWritable, LongWritable, Point2DWritable > {
		@Override
		public void map(LongWritable key, Point2DWritable value, Context context
				) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class InputReducer extends Reducer<LongWritable, Point2DWritable, NullWritable,  Text> {
		static Text txt = new Text();
		@Override
		public void reduce(LongWritable key, Iterable<Point2DWritable> values, Context context) 
				throws IOException, InterruptedException {
			StringBuilder tmp = new StringBuilder();
			for (Point2DWritable i: values) {
				tmp.append("(" + i.p.x + "," + i.p.y + ")\n");
			}
			txt.set(tmp.toString());
			context.write(null, txt);      
		}
	}
	
	public void usage() {
		System.out.println("Invalid arguments, waiting for 4 parameters:  nbMapper nbReducer nbPointsPerMapper output_file");
	}
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		int nbReducer = 1;
		int nbSplits = 1;
		int pointsPerSplits = 10;
		String filename = "misc";
		try {
			nbSplits = Integer.parseInt(args[0]);
			nbReducer = Integer.parseInt(args[1]);
			pointsPerSplits = Integer.parseInt(args[2]);
			filename = args[3];
		}
		catch(Exception e) {
			usage();
			return -1;
		}

		 Path workPath = new Path(filename+"TMP");
		 Path dstPath = new Path(filename);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Random Points Generator");
		job.setJarByClass(TPInputFormat.class);		
		job.setNumReduceTasks(nbReducer);
		job.setInputFormatClass(RandomInputFormat.class);
		RandomInputFormat.setRandomSplits(nbSplits);
		RandomInputFormat.setPointPerSpits(pointsPerSplits);
		job.setMapperClass(IdentityMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Point2DWritable.class);
		job.setReducerClass(InputReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, workPath);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
				
		 FileSystem fs = FileSystem.get(conf);
		 FileUtil.copyMerge(fs, workPath, fs, dstPath, true, conf, "");

		 
		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new PointsGenerator(), args));
	}

}
