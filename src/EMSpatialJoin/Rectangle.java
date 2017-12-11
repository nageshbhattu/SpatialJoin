package EMSpatialJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Rectangle  implements Writable, Comparable<Rectangle>{
	
	// Coordinates of the rectangle
	double x1,y1,x2,y2;
	int relationIndex;
	int rowNum;
	/**
	 * Used in sorting the data in the reducer
	 */
	public Rectangle() {
		
	}
	public Rectangle(int rowNum, int relIndex,double x1, double y1, double x2,double y2) {
		this.rowNum = rowNum;
		this.relationIndex=relIndex;
		this.x1= x1;
		this.y1= y1;
		this.x2 = x2;
		this.y2 = y2;
	}
	public Rectangle(Rectangle r) {
		this.rowNum = r.rowNum;
		this.relationIndex = r.relationIndex;
		this.x1 = r.x1;
		this.x2 = r.x2;
		this.y1 = r.y1;
		this.y2 = r.y2;
	}
	@Override
	public int compareTo(Rectangle arg0) {
		// TODO Auto-generated method stub
		if(relationIndex<arg0.relationIndex)
			return -1;
		else if(relationIndex>arg0.relationIndex)
			return 1;
		else {
			if(x1<arg0.x1)
				return -1;
			else if(x1>arg0.x1)
				return 1;
			else {
				if(y1<arg0.y1)
					return -1;
				else if(y1>arg0.y1)
					return 1;
				else
					return 0;
			}
		}	
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		rowNum = in.readInt();
		relationIndex = in.readInt();
		x1 = in.readDouble();
		y1 = in.readDouble();
		x2 = in.readDouble();
		y2 = in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(rowNum);
		out.writeInt(relationIndex);
		out.writeDouble(x1);
		out.writeDouble(y1);
		out.writeDouble(x2);
		out.writeDouble(y2);
	}
	@Override
	public String toString() {
		return rowNum + "," +relationIndex+"," + x1 + ","+y1 + "," + x2 + ","+y2;
	}

}

