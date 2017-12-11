package Sequential;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ABJoinTuple  implements Writable, Comparable<ABJoinTuple>{
	// Coordinates of the rectangle
	int abType;
        int JoinType ;
	int r1RowNum;
	int r2RowNum;
	int r1RelationIndex;
	int r2RelationIndex;
	double r1x1;
	double r1x2;
	double r1y1;
	double r1y2;
	double r2x1;
	double r2x2;
	double r2y1;
	double r2y2;
	
	/**
	 * Used in sorting the data in the reducer
	 */
	public ABJoinTuple() {
		
	}
	public ABJoinTuple(int abType, int JoinType, int r1RowNum, int r1RelationIndex, double r1x1, double r1y1, double r1x2, double r1y2,
			int r2RowNum, int r2RelationIndex, double r2x1, double r2y1, double r2x2, double r2y2) {
		this.abType = abType;
                this.JoinType = JoinType;
		this.r1RowNum = r1RowNum;
		this.r2RowNum = r2RowNum;
		this.r1RelationIndex= r1RelationIndex;
		this.r2RelationIndex= r2RelationIndex;
		this.r1x1 =r1x1  ;
		this.r1x2 = r1x2;
		this.r1y1 = r1y1;
		this.r1y2 = r1y2;
		this.r2x1 = r2x1;
		this.r2x2  = r2x2;
		this.r2y1 = r2y1;
		this.r2y2  = r2y2;
	}
	public ABJoinTuple(ABJoinTuple jt ) {
		this.abType = jt.abType;
                this.JoinType = jt.JoinType;
		this.r1RowNum = jt.r1RowNum;
		this.r2RowNum = jt.r2RowNum;
		this.r1RelationIndex= jt.r1RelationIndex;
		this.r2RelationIndex= jt.r2RelationIndex;
		this.r1x1 = jt.r1x1  ;
		this.r1x2 = jt.r1x2;
		this.r1y1 = jt.r1y1;
		this.r1y2 = jt.r1y2;
		this.r2x1 = jt.r2x1;
		this.r2x2 = jt.r2x2;
		this.r2y1 = jt.r2y1;
		this.r2y2 = jt.r2y2;
	}
	@Override
	public int compareTo(ABJoinTuple arg0) {
		// TODO Auto-generated method stub
		if(JoinType<arg0.JoinType)
			return -1;
		else if(JoinType>arg0.JoinType)
			return 1;
		else {
			return 0;
			 }
		}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		abType = in.readInt();
                JoinType = in.readInt();
		r1RowNum = in.readInt();
		r2RowNum = in.readInt();
		r1RelationIndex = in.readInt();
		r2RelationIndex= in.readInt();
		r1x1 = in.readDouble();
		r1x2 = in.readDouble();
		r1y1 = in.readDouble();
		r1y2 = in.readDouble();
		r2x1 = in.readDouble();
		r2x2 = in.readDouble();
		r2y1 = in.readDouble();
		r2y2 = in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(abType);
                out.writeInt(JoinType);
		out.writeInt(r1RowNum);
		out.writeInt(r2RowNum);
		out.writeInt(r1RelationIndex);
		out.writeInt(r2RelationIndex);
		out.writeDouble(r1x1);
		out.writeDouble(r1x2);
		out.writeDouble(r1y1);
		out.writeDouble(r1y2);
		out.writeDouble(r2x1);
		out.writeDouble(r2x2);
		out.writeDouble(r2y1);
		out.writeDouble(r2y2);
	}
	@Override
	public String toString() {
		return JoinType + ","+r1RowNum+ ","+r1RelationIndex+","+r1x1+","+r1y1+","+r1x2+","+r1y2+ ","+r2RowNum+ ","+r2RelationIndex+","+r2x1+","+r2y1+","+r2x2+","+r2y2;
	}
}