package emsmain;

import ControlledRepNew.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ControlledRepJoinTuple  implements Writable, Comparable<ControlledRepJoinTuple>
{
	// Coordinates of the rectangle
	int JoinType ;
	int r1RowNum;   int r1RelationIndex;
	int r2RowNum;   int r2RelationIndex;
	int r3RowNum;   int r3RelationIndex;
        int r4RowNum;   int r4RelationIndex;
	
	double r1x1;	double r1x2;
	double r1y1;	double r1y2;
        
	double r2x1;	double r2x2;
	double r2y1;	double r2y2;
	
        double r3x1;	double r3x2;
	double r3y1;	double r3y2;
	
        double r4x1;	double r4x2;
	double r4y1;	double r4y2;
	
	/**
	 * Used in sorting the data in the reducer
	 */
	public ControlledRepJoinTuple() {
		
	}
	public ControlledRepJoinTuple(int JoinType, int r1RowNum, int r1RelationIndex, double r1x1, double r1y1, double r1x2, double r1y2,
                                                    int r2RowNum, int r2RelationIndex, double r2x1, double r2y1, double r2x2, double r2y2,
                                                    int r3RowNum, int r3RelationIndex, double r3x1, double r3y1, double r3x2, double r3y2,
                                                    int r4RowNum, int r4RelationIndex, double r4x1, double r4y1, double r4x2, double r4y2)
        {
		this.JoinType = JoinType;
		this.r1RowNum = r1RowNum;   this.r1RelationIndex= r1RelationIndex;
		
		this.r1x1 =r1x1;    this.r1x2 = r1x2;
		this.r1y1 = r1y1;   this.r1y2 = r1y2;   this.r2RowNum = r2RowNum;   this.r2RelationIndex= r2RelationIndex;
		this.r2x1 = r2x1;   this.r2x2  = r2x2;
		this.r2y1 = r2y1;   this.r2y2  = r2y2;  this.r3RowNum = r3RowNum;   this.r3RelationIndex= r3RelationIndex;
                this.r3x1 = r3x1;   this.r3x2  = r3x2;
		this.r3y1 = r3y1;   this.r3y2  = r3y2;  this.r4RowNum = r4RowNum;   this.r4RelationIndex= r4RelationIndex;
                this.r4x1 = r4x1;   this.r4x2  = r4x2;
		this.r4y1 = r4y1;   this.r4y2  = r4y2;
	
	}
	public ControlledRepJoinTuple(ControlledRepJoinTuple jt ) {
		this.JoinType = jt.JoinType;
		this.r1RowNum = jt.r1RowNum;    this.r1RelationIndex= jt.r1RelationIndex;
		
		this.r1x1 = jt.r1x1;    this.r1x2 = jt.r1x2;
		this.r1y1 = jt.r1y1;	this.r1y2 = jt.r1y2;    this.r2RowNum = jt.r2RowNum;	this.r2RelationIndex= jt.r2RelationIndex;
		this.r2x1 = jt.r2x1;	this.r2x2 = jt.r2x2;
		this.r2y1 = jt.r2y1;	this.r2y2 = jt.r2y2;    this.r3RowNum = jt.r3RowNum;	this.r3RelationIndex= jt.r3RelationIndex;
                this.r3x1 = jt.r3x1;	this.r3x2 = jt.r3x2;
		this.r3y1 = jt.r3y1;	this.r3y2 = jt.r3y2;    this.r4RowNum = jt.r4RowNum;	this.r4RelationIndex= jt.r4RelationIndex;
                this.r4x1 = jt.r4x1;	this.r4x2 = jt.r4x2;
		this.r4y1 = jt.r4y1;	this.r4y2 = jt.r4y2;
        }
	@Override
	public int compareTo(ControlledRepJoinTuple arg0) 
        {
		// TODO Auto-generated method stub
            /*	if(JoinType<arg0.JoinType)
			return -1;
		else if(JoinType>arg0.JoinType)
			return 1;
		else{
			return 0;
                    }   */
            if(r1RowNum < arg0.r1RowNum)
			return -1;
		else if(r1RowNum > arg0.r1RowNum)
			return 1;
		else {
                    return r1RowNum-arg0.r1RowNum;
                }
	}
   /*     public boolean equals(ControlledRepJoinTuple jt)
        {
            if(jt instanceof ControlledRepJoinTuple)
            {
                ControlledRepJoinTuple newjt = (ControlledRepJoinTuple) jt;
                if(this.r1RowNum == newjt.r1RowNum)
                    return true;
            }return false;
        }
*/

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		JoinType = in.readInt();
		r1RowNum = in.readInt();    r1RelationIndex = in.readInt();
		
                r1x1 = in.readDouble(); r1x2 = in.readDouble();
		r1y1 = in.readDouble();	r1y2 = in.readDouble(); r2RowNum = in.readInt();    r2RelationIndex= in.readInt();
		r2x1 = in.readDouble();	r2x2 = in.readDouble();
		r2y1 = in.readDouble();	r2y2 = in.readDouble(); r3RowNum = in.readInt();    r3RelationIndex= in.readInt();
		r3x1 = in.readDouble();	r3x2 = in.readDouble();
		r3y1 = in.readDouble();	r3y2 = in.readDouble(); r4RowNum = in.readInt();    r4RelationIndex= in.readInt();
		r4x1 = in.readDouble();	r4x2 = in.readDouble();
		r4y1 = in.readDouble();	r4y2 = in.readDouble();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(JoinType);
		out.writeInt(r1RowNum);	out.writeInt(r1RelationIndex);	
                
		out.writeDouble(r1x1);  out.writeDouble(r1x2);
		out.writeDouble(r1y1);	out.writeDouble(r1y2);  out.writeInt(r2RowNum); out.writeInt(r2RelationIndex);
		out.writeDouble(r2x1);	out.writeDouble(r2x2);
		out.writeDouble(r2y1);	out.writeDouble(r2y2);  out.writeInt(r3RowNum);   out.writeInt(r3RelationIndex);
                out.writeDouble(r3x1);  out.writeDouble(r3x2);  
		out.writeDouble(r3y1);  out.writeDouble(r3y2);  out.writeInt(r3RowNum);   out.writeInt(r3RelationIndex);
                out.writeDouble(r4x1);  out.writeDouble(r4x2);
		out.writeDouble(r4y1);  out.writeDouble(r4y2);
	}
	@Override
	public String toString() {
		return JoinType + ","+r1RowNum+ ","+r1RelationIndex+ ","+r1x1+","+r1y1+","+r1x2+","+r1y2+ ","+r2RowNum+ ","+r2RelationIndex+","+r2x1+","+r2y1+","+r2x2+","+r2y2+ ","+r3RowNum+ ","+r3RelationIndex+","+r3x1+","+r3y1+","+r3x2+","+r3y2+ ","+r4RowNum+ ","+r4RelationIndex+","+r4x1+","+r4y1+","+r4x2+","+r4y2;
	}
}

