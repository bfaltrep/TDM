import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP4 {

	public static class PopWritable implements Writable
	{
		public int _sum;
		public int _count;

		public int _min;
		public int _max;

		public PopWritable(){}

		public PopWritable(int sum, int count, int min, int max)
		{
			_sum = sum;
			_count = count;

			_min = min;
			_max = max;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(_sum);
			out.writeInt(_count);

			out.writeInt(_min);
			out.writeInt(_max);

		}

		public void readFields(DataInput in) throws IOException {
			_sum = in.readInt();
			_count = in.readInt();

			_min = in.readInt();
			_max = in.readInt();
		}
	}



	public static class TP4Mapper extends Mapper<Object, Text, IntWritable, PopWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
						
			double step = Double.parseDouble(context.getConfiguration().get("step"));
			
			try
			{
				if(!tokens[4].matches(""))
				{
					int pop = Integer.parseInt(tokens[4]);
					if(pop < 1) return;
				
					context.write(new IntWritable((int)Math.pow(step,1+Math.floor(Math.log(pop) / Math.log(step)))),new PopWritable(pop,1,pop,pop));
					//version pré-exercice3 : context.write(new IntWritable((int)Math.pow(10.0,1+Math.floor(Math.log10(pop)))),new PopWritable(pop,1,pop,pop));
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	public static class TP4Combiner extends Reducer<IntWritable, PopWritable, IntWritable, PopWritable>{
		public void reduce(IntWritable key, Iterable<PopWritable> values, Context context) throws IOException, InterruptedException {			
			int count = 0;
			int sum = 0;
			int min = Integer.MAX_VALUE;
			int max = 0;

			for(PopWritable each : values)
			{
				sum += each._sum;
				if(min>each._min)
					min=each._min;
				if(max<each._max)
					max=each._max;
				count++;
			}

			context.write(key, new PopWritable(sum,count,min,max));
		}
	}

	public static class TP4Reducer extends Reducer<IntWritable, PopWritable, Text, Text> {

		public void reduce(IntWritable key, Iterable<PopWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			int sum = 0;
			int min = Integer.MAX_VALUE;
			int max = 0;

			for(PopWritable each : values)
			{
				sum += each._sum;
				count+=each._count;
				if(min>each._min)
					min=each._min;
				if(max<each._max)
					max=each._max;
			}

			String choosed = context.getConfiguration().get("choosed");
			
			if(choosed.matches("count"))
				context.write(new Text(Integer.toString(key.get())), new Text(Integer.toString(count)));
			else
				context.write(new Text(Integer.toString(key.get())), new Text(Integer.toString(count)+"\t\t"+Integer.toString(sum/count)+"\t\t"+Integer.toString(max)+"\t\t"+Integer.toString(min)));
			
				
				
		}


		@Override
		public void setup (Context context) throws IOException, InterruptedException
		{
			String choosed = context.getConfiguration().get("choosed");
			
			if(choosed.matches("count"))
				context.write(new Text("Class"),new Text("Count"));
			else
				context.write(new Text("Class"),new Text("Count\t\tAvg\t\tMax\t\tMin"));
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 3)
		{
			System.out.println("usage : yarn jar ourApplication.jar inputfile outputdir [\"count\" | \"all\"] step\n\n\t count : count cities number by step.\n\t all : write all our informations about cities population.\n\n\tstep is a double which define our granularity.");
			return;
		}
		
		Configuration conf = new Configuration();
		//Arguments
		conf.set("choosed", args[2]);
		if(args.length > 3 && Double.parseDouble(args[3]) > 1.0)
			conf.set("step", args[3]);
		else
			conf.set("step", Double.toString(10.0));
		
		Job job = Job.getInstance(conf, "TP4");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP4.class);

		job.setMapperClass(TP4Mapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PopWritable.class);
		job.setCombinerClass(TP4Combiner.class);

		job.setReducerClass(TP4Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
