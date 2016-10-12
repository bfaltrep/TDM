import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable{

	private long _length;
	
	public FakeInputSplit(){
		_length = 100000;
	}
	
	public FakeInputSplit(long length) {
		_length = length;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return _length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{};
	}

	public void write(DataOutput out) throws IOException {}

	public void readFields(DataInput in) throws IOException {}

}
