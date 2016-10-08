import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class RandomPointInputFormat extends InputFormat<Object, Object>{

	@Override
	public RecordReader createRecordReader(InputSplit arg0,	TaskAttemptContext arg1) throws IOException, InterruptedException {
		
		RandomPointReader rpr = new RandomPointReader();
		return rpr;
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
		
		int nb_splits = Integer.parseInt(arg0.getConfiguration().get("nb_mapper"));
		long size_split = Integer.parseInt(arg0.getConfiguration().get("point_by_split"));
		
		List<InputSplit> list = new ArrayList<InputSplit>(nb_splits);
		for(int i = 0 ; i < nb_splits ; i++)
			list.add(new FakeInputSplit(size_split));
		
		return list;
	}

}
