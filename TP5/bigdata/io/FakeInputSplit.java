/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférences HDR
 * LaBRI: Université de Bordeaux
 */
package bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable {
	private long _length;
	
	public FakeInputSplit() {
	}
	
	public FakeInputSplit(long nbpoints) {
		_length = nbpoints;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return _length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	public void readFields(DataInput arg0) throws IOException {
		_length = arg0.readLong();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(_length);
	}
}

