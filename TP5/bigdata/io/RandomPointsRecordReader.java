/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférencces HDR
 * LaBRI: Université de Bordeaux
 */
package bigdata.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointsRecordReader extends RecordReader<LongWritable, Point2DWritable> {

	private LongWritable curKey = new LongWritable(0);
	private Point2DWritable curPoint = new Point2DWritable();
	private long nbPoints = 10000;

	@Override
	public void close() throws IOException {
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		if (!(curKey.get() < this.nbPoints)) throw new IOException("no more points");
		return curKey;
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
		if (!(curKey.get() < this.nbPoints)) throw new IOException("no more points");
		return curPoint;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)((double)curKey.get()/(double)this.nbPoints);
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext context) throws IOException, InterruptedException {
		this.nbPoints = arg0.getLength();
		curKey.set(-1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		curKey.set(curKey.get()+1);
		curPoint.p.x = Math.random();
		curPoint.p.y = Math.random();
		return curKey.get() < this.nbPoints;
	}

}