import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Point2DWritable implements Writable{
	public Point2D.Double _point;
	
	public Point2DWritable(){
		_point = new Point2D.Double();
		_point.x = 0;
		_point.y = 0;
	}
	
	public Point2DWritable(double x, double y){
		_point = new Point2D.Double();
		_point.x = x;
		_point.y = y;
	}
	
	public Point2D.Double getPoint(){
		return _point;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(_point.x);
		out.writeDouble(_point.y);
	}

	public void readFields(DataInput in) throws IOException {
		_point.x = in.readDouble();
		_point.y = in.readDouble();
	}
	
}
