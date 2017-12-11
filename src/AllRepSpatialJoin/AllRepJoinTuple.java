package AllRepSpatialJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AllRepJoinTuple  implements Writable, Comparable<AllRepJoinTuple>
{
	// Coordinates of the rectangle
//	int JoinType ;
	int r1RowNum;
	//int r2RowNum;
	int r1RelationIndex;
	double r1x1;
	double r1x2;
	double r1y1;
	double r1y2;
	public AllRepJoinTuple() {
		
	}
	public AllRepJoinTuple(int r1RowNum, int r1RelationIndex, double r1x1, double r1y1, double r1x2, double r1y2)
        {
		this.r1RowNum = r1RowNum;
                this.r1RelationIndex = r1RelationIndex;
		this.r1x1 =r1x1  ;
		this.r1x2 = r1x2;
		this.r1y1 = r1y1;
		this.r1y2 = r1y2;
	}
	public AllRepJoinTuple(AllRepJoinTuple jt ) {
		this.r1RowNum = jt.r1RowNum;
		this.r1RelationIndex = jt.r1RelationIndex;
		this.r1x1 = jt.r1x1;
		this.r1x2 = jt.r1x2;
		this.r1y1 = jt.r1y1;
		this.r1y2 = jt.r1y2;
	}
	@Override
/*	public int compareTo(AllRepJoinTuple arg0) 
        {
		// TODO Auto-generated method stub
		if(JoinType<arg0.JoinType)
			return -1;
		else if(JoinType>arg0.JoinType)
			return 1;
		else{
			return 0;
                    }
	}
*/
//	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		r1RelationIndex = in.readInt();
		r1RowNum = in.readInt();
		r1x1 = in.readDouble();
		r1x2 = in.readDouble();
		r1y1 = in.readDouble();
		r1y2 = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(r1RelationIndex);
		out.writeInt(r1RowNum);
		out.writeDouble(r1x1);
		out.writeDouble(r1x2);
		out.writeDouble(r1y1);
		out.writeDouble(r1y2);
	}
	@Override
	public String toString() {
		return r1RowNum+ ","+r1x1+","+r1y1+","+r1x2+","+r1y2;
	}

    @Override
    public int compareTo(AllRepJoinTuple o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}

