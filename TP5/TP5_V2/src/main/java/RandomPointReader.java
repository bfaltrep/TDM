import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class RandomPointReader extends RecordReader<LongWritable, Point2DWritable>{

	private Long _key;
	private Point2DWritable _point;
	private long _length;
	
	public RandomPointReader(){
		_key = (long)0;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(_key);
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
		return _point;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (_key*100)/_length;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		_length = arg0.getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean res = !(_key==_length);
		if(res)
		{
			_key+=(long)1.0;
			_point = new Point2DWritable(Math.random(),Math.random());
			
		}
		return res;
	}
}
