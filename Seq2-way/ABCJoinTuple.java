package abcSequential;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ABCJoinTuple  implements Writable, Comparable<ABCJoinTuple>
{
    // Coordinates of the rectangle
    //int abcType;
    int JoinType ;
    int r1RowNum;
    int r2RowNum;
    int r3RowNum;
    int r1RelationIndex;
    int r2RelationIndex;
    int r3RelationIndex;
    double r1x1;
    double r1x2;
    double r1y1;
    double r1y2;
    double r2x1;
    double r2x2;
    double r2y1;
    double r2y2;
    double r3x1;
    double r3x2;
    double r3y1;
    double r3y2;

    /**
     * Used in sorting the data in the reducer
     */
    public ABCJoinTuple() {

    }
    public ABCJoinTuple( int JoinType, int r1RowNum, int r1RelationIndex, double r1x1, double r1y1, double r1x2, double r1y2,
                    int r2RowNum, int r2RelationIndex, double r2x1, double r2y1, double r2x2, double r2y2,
                    int r3RowNum, int r3RelationIndex, double r3x1, double r3y1, double r3x2, double r3y2) 
    {
      //  this.abcType = abcType;
        this.JoinType = JoinType;
        this.r1RowNum = r1RowNum;
        this.r2RowNum = r2RowNum;
        this.r3RowNum = r3RowNum;
        this.r1RelationIndex= r1RelationIndex;
        this.r2RelationIndex= r2RelationIndex;
        this.r3RelationIndex= r3RelationIndex;
        this.r1x1 =r1x1  ;
        this.r1x2 = r1x2;
        this.r1y1 = r1y1;
        this.r1y2 = r1y2;
        this.r2x1 = r2x1;
        this.r2x2 = r2x2;
        this.r2y1 = r2y1;
        this.r2y2 = r2y2;
        this.r3x1 = r3x1;
        this.r3x2 = r3x2;
        this.r3y1 = r3y1;
        this.r3y2 = r3y2;
    }
    public ABCJoinTuple(ABCJoinTuple jt ) {
       // this.abcType = jt.abcType;
        this.JoinType = jt.JoinType;
        this.r1RowNum = jt.r1RowNum;
        this.r2RowNum = jt.r2RowNum;
        this.r3RowNum = jt.r3RowNum;
        this.r1RelationIndex= jt.r1RelationIndex;
        this.r2RelationIndex= jt.r2RelationIndex;
        this.r3RelationIndex= jt.r3RelationIndex;
        this.r1x1 = jt.r1x1  ;
        this.r1x2 = jt.r1x2;
        this.r1y1 = jt.r1y1;
        this.r1y2 = jt.r1y2;
        this.r2x1 = jt.r2x1;
        this.r2x2 = jt.r2x2;
        this.r2y1 = jt.r2y1;
        this.r2y2 = jt.r2y2;
        this.r3x1 = jt.r3x1;
        this.r3x2 = jt.r3x2;
        this.r3y1 = jt.r3y1;
        this.r3y2 = jt.r3y2;
    }
    @Override
    public int compareTo(ABCJoinTuple arg0) {
        // TODO Auto-generated method stub
        if(JoinType<arg0.JoinType)
            return -1;
        else if(JoinType>arg0.JoinType)
            return 1;
        else 
            return 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
      //  abcType = in.readInt();
        JoinType = in.readInt();
        r1RowNum = in.readInt();
        r2RowNum = in.readInt();
        r3RowNum = in.readInt();
        r1RelationIndex = in.readInt();
        r2RelationIndex= in.readInt();
        r3RelationIndex= in.readInt();
        r1x1 = in.readDouble();
        r1x2 = in.readDouble();
        r1y1 = in.readDouble();
        r1y2 = in.readDouble();
        r2x1 = in.readDouble();
        r2x2 = in.readDouble();
        r2y1 = in.readDouble();
        r2y2 = in.readDouble();
        r3x1 = in.readDouble();
        r3x2 = in.readDouble();
        r3y1 = in.readDouble();
        r3y2 = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
      //  out.writeInt(abcType);
        out.writeInt(JoinType);
        out.writeInt(r1RowNum);
        out.writeInt(r2RowNum);
        out.writeInt(r3RowNum);
        out.writeInt(r1RelationIndex);
        out.writeInt(r2RelationIndex);
        out.writeInt(r3RelationIndex);
        out.writeDouble(r1x1);
        out.writeDouble(r1x2);
        out.writeDouble(r1y1);
        out.writeDouble(r1y2);
        out.writeDouble(r2x1);
        out.writeDouble(r2x2);
        out.writeDouble(r2y1);
        out.writeDouble(r2y2);
        out.writeDouble(r3x1);
        out.writeDouble(r3x2);
        out.writeDouble(r3y1);
        out.writeDouble(r3y2);
    }
    @Override
    public String toString() {
        return JoinType + ","+r1RowNum+ ","+r1RelationIndex+","+r1x1+","+r1y1+","+r1x2+","+r1y2+ ","+r2RowNum+ ","+r2RelationIndex+","+r2x1+","+r2y1+","+r2x2+","+r2y2+ ","+r3RowNum+","+r3RelationIndex+","+r3x1+","+r3y1+","+r3x2+","+r3y2;
    }
}