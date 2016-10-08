import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Point2DWritable {
public Point2D.Double _point;
	
	public Point2DWritable(){}
	
	public Point2DWritable(double x, double y){
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
