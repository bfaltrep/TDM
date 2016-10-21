import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TP5_efficient extends Configured implements Tool {

	public static class TP5Mapper extends Mapper<LongWritable, Point2DWritable, NullWritable, NullWritable > {
		@Override
		public void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
			if (value.getPoint().x*value.getPoint().x + value.getPoint().y*value.getPoint().y < 1.)
				context.getCounter("Pi", "in").increment(1);
			else
				context.getCounter("Pi", "out").increment(1);
		}
	}
	
	public int run(String[] args) throws Exception {
		int nbSplits = 1;
		int pointsPerSplits = 10;
		try {
			nbSplits = Integer.parseInt(args[0]);
			pointsPerSplits = Integer.parseInt(args[1]);
		}
		catch(Exception e) {
			System.out.println("Invalid arguments, waiting for 2 parameters:  nbSplits nbPointsPerSplits");
			return -1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "InputFormat compute PI");
		job.setNumReduceTasks(0);
		job.setJarByClass(RandomPointInputFormat.class); // What ?
		RandomPointInputFormat.setRandomSplits(nbSplits);
		RandomPointInputFormat.setPointPerSpits(pointsPerSplits);
		job.setInputFormatClass(RandomPointInputFormat.class);
		job.setMapperClass(TP5Mapper.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.waitForCompletion(true);
		long inc = job.getCounters().findCounter("Pi","in").getValue();
		long outc = job.getCounters().findCounter("Pi","out").getValue();
		
		System.out.println("Pi = " + (double)inc * 4. / (double)(outc + inc));
		
		return 0;
	}

	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new TP5_efficient(), args));
	}
}
