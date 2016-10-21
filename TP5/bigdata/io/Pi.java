/**
 * @author David Auber 
 * @date 07/10/2016
 * Ma�tre de conf�rences HDR
 * LaBRI: Universit� de Bordeaux
 */
package bigdata.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Pi extends Configured implements Tool {


	public static class PiMapper extends Mapper<LongWritable, Point2DWritable, NullWritable, NullWritable > {
		@Override
		public void map(LongWritable key, Point2DWritable value, Context context
				) throws IOException, InterruptedException {
			if (value.p.x*value.p.x + value.p.y*value.p.y < 1.)
				context.getCounter("Pi", "in").increment(1);
			else
				context.getCounter("Pi", "out").increment(1);
		}
	}

	public void usage() {
		System.out.println("Invalid arguments, waiting for 2 parameters:  nbSplits nbPointsPerSplits");
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		int nbSplits = 1;
		int pointsPerSplits = 10;
		try {
			nbSplits = Integer.parseInt(args[0]);
			pointsPerSplits = Integer.parseInt(args[1]);
		}
		catch(Exception e) {
			usage();
			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "InputFormat compute PI");
		job.setNumReduceTasks(0);
		job.setJarByClass(TPInputFormat.class);
		RandomInputFormat.setRandomSplits(nbSplits);
		RandomInputFormat.setPointPerSpits(pointsPerSplits);
		job.setInputFormatClass(RandomInputFormat.class);
		job.setMapperClass(PiMapper.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.waitForCompletion(true);
		long inc = job.getCounters().findCounter("Pi","in").getValue();
		long outc = job.getCounters().findCounter("Pi","out").getValue();
		System.out.println("Pi = " + (double)inc * 4. / (double)(outc + inc));
		return 0;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Pi(), args));
	}

}
