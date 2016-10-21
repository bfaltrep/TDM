/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférencces HDR
 * LaBRI: Université de Bordeaux
 */
package bigdata.io;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point2DWritable implements Writable {
	public Point2D.Double p;
	public Point2DWritable() {
		p = new Point2D.Double();
	}
	public void readFields(DataInput in) throws IOException {
		p.x = in.readDouble();
		p.y = in.readDouble();
	}
	public void write(DataOutput out) throws IOException {
		out.writeDouble(p.x);
		out.writeDouble(p.y);
	}
}