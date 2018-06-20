/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package emsmain;
package Common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author hduser
 */
public class Pair implements Writable, Comparable<Pair> {
    
    public int r1RowNum;
    public int r2RowNum;
    //int joinType;
    public Pair(){
        
    }
    public Pair(Pair tri){
        this.r1RowNum = r1RowNum;
        this.r2RowNum = r2RowNum;
        //this.joinType = joinType;
    }
    public Pair(int r1RowNum, int r2RowNum){
        this.r1RowNum = r1RowNum;
        this.r2RowNum = r2RowNum;
       // this.joinType = joinType;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(r1RowNum);
        out.writeInt(r2RowNum);
      //  out.writeInt(joinType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        r1RowNum = in.readInt();
        r2RowNum = in.readInt();
      //  joinType = in.readInt();
        
    }

    @Override
    public int compareTo(Pair arg0) {
        if(this.r1RowNum!=arg0.r1RowNum)
            return this.r1RowNum - arg0.r1RowNum;
        return this.r2RowNum - arg0.r2RowNum;
    }
    
}
