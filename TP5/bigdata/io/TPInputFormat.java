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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TPInputFormat {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("generator", bigdata.io.PointsGenerator.class, "a map/reduce program that generates random points into a file");
			pgd.addClass("pi", bigdata.io.Pi.class, "a map/reduce program that computes Pi using Montecarlo algorithm");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
