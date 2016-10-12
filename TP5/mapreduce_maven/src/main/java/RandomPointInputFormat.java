import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class RandomPointInputFormat extends InputFormat<Object, Object>{

	private static int randomSplits = 1;
	private static int nbPointsPerSplit = 1;
	
	@Override
	public RecordReader createRecordReader(InputSplit arg0,	TaskAttemptContext arg1)
				throws IOException, InterruptedException {
		RandomPointReader rpr = new RandomPointReader(arg0.getLength());
		return rpr;
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {

		List<InputSplit> list = new ArrayList<InputSplit>(randomSplits);
		for(int i = 0 ; i < randomSplits ; i++)
			list.add(new FakeInputSplit(nbPointsPerSplit));
		
		return list;
	}

	public static void setRandomSplits(int nb) {
		if (nb > 0) randomSplits = nb;
	}

	public static void setPointPerSpits(int nb) {
		if (nb > 0) nbPointsPerSplit = nb;
	}
	
}
