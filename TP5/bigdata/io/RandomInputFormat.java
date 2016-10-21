/**
 * @author David Auber 
 * @date 07/10/2016
 * Ma�tre de conf�rencces HDR
 * LaBRI: Universit� de Bordeaux
 */
package bigdata.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomInputFormat extends InputFormat<LongWritable, Point2DWritable> {
	private static int randomSplits = 1;
	private static int nbPointsPerSplit = 1;
	
	@Override
	public RecordReader<LongWritable, Point2DWritable> createRecordReader(InputSplit arg0, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RandomPointsRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
		ArrayList<InputSplit> result = new ArrayList<InputSplit>(randomSplits);
		for (int i=0;i<randomSplits;++i)
			result.add(new FakeInputSplit(nbPointsPerSplit));
		return result;
	}

	public static void setRandomSplits(int nb) {
		if (nb > 0) randomSplits = nb;
	}

	public static void setPointPerSpits(int nb) {
		if (nb > 0) nbPointsPerSplit = nb;
	}
}


