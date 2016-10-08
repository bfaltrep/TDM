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
		RandomPointReader rpr = new RandomPointReader(arg0.getLength());
		return rpr;
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
		
		int nb_splits = arg0.getConfiguration().getInt("nb_mapper", -1);
		long size_split = arg0.getConfiguration().getLong("point_by_split",-1);
		
		if(nb_splits == -1)
			nb_splits = 2;
		if(size_split == -1) // problème : à l'heure actuelle, n'est pas considéré ensuite.
			size_split = 5000;
		
		
		List<InputSplit> list = new ArrayList<InputSplit>(nb_splits);
		for(int i = 0 ; i < nb_splits ; i++)
			list.add(new FakeInputSplit(size_split));
		
		return list;
	}

}
