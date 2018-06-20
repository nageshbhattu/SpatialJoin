package abcSequential;
import Common.*;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<LongWritable, ABJoinTuple, LongWritable, Text> 
{
    long startr1;
    @Override
    protected void setup(Context context) throws IOException{
        startr1 = System.currentTimeMillis();
    }
    @Override
    public void reduce(LongWritable key, Iterable<ABJoinTuple> values, Context context) throws IOException, InterruptedException 
    {
        ArrayList<ABJoinTuple> lb = new ArrayList<>();
        ArrayList<ABJoinTuple> lc = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        
        for (ABJoinTuple t : values) 
        {
            //ABJoinTuple newt = new ABJoinTuple(t);
            switch (t.JoinType) {
                case 0:
                    lc.add(t);
                    break;
                case 1:
                    lb.add(t);
                    break;
                default:
                    break;
            }
        }
        join(cellNumber, cellRow, cellsPerRow, cellCol, lb, lc, 2, cellRect,context);
    }
    public void join(int CellNumber, int cellRow, int cellsPerRow, int cellCol, ArrayList<ABJoinTuple> lb, ArrayList<ABJoinTuple> lc, int joinType, Rectangle cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < lb.size(); i++) 
        {
            ABJoinTuple r1 = lb.get(i);
            for (int j = 0; j < lc.size(); j++) 
            {
                boolean overlaps = false;
                ABJoinTuple r2 = lc.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                if (r1.r2x1 <= r2.r1x1 && r1.r2x2 >= r2.r1x1 && r1.r2x2 < r2.r1x2) {
    
                    if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r2.r1y2 > r1.r2y2) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } // Case 2 
                    else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r2.r1y2 <= r1.r2y2) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } // Case 3: 
                    else if (r1.r2y1 <= r2.r1y2 && r1.r2y2 > r2.r1y2 && r2.r1y1 < r1.r2y1) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } //Case 4:
                    else if (r1.r2y2 <= r2.r1y2 && r2.r1y1 < r1.r2y1) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } // Case 2: r2.x1 is less than r1.x1
                }    
                else if (r2.r1x1 <= r1.r2x1 && r2.r1x2 >= r1.r2x1 && r2.r1x2< r1.r2x2)//changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                          //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
                {
                    if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r2.r1y2 > r1.r2y2) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r2.r1y2 <= r1.r2y2) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r2y1 <= r2.r1y2 && r1.r2y2 > r2.r1y2 && r2.r1y1 < r1.r2y1) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 < r1.r2y1 && r2.r1y2 > r1.r2y1) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    }
                }   //case 3: 
                else if (r2.r1x1 < r1.r2x1 && r2.r1x2 > r1.r2x2) {
                
                    if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r2.r1y2 > r1.r2y2) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r2.r1y2 <= r1.r2y2) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r2y1 <= r2.r1y2 && r1.r2y2 > r2.r1y2 && r2.r1y1 <= r1.r2y1) {//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 < r1.r2y1 && r2.r1y2 > r1.r2y1) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    }
                }//Case 4:
                else if (r2.r1x1 > r1.r2x1 && r2.r1x2 < r1.r2x2) {

                    if (r1.r2y1 >= r2.r1y1 && r1.r2y1 < r2.r1y2 && r2.r1y2 < r1.r2y2) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r2y1 && r1.r2y1 < r2.r1y2 && r1.r2y2 <= r2.r1y2) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 > r2.r1y1 && r1.r2y2 <= r2.r1y2) { 
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } else if (r1.r2y1 < r2.r1y1 && r1.r2y2 > r2.r1y1) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    }
                }//Case 5:
                else if (r2.r1x1 == r1.r2x1 && r2.r1x2 == r1.r2x2) {

                    if (r1.r2y2 > r2.r1y2 && r1.r2y1 <= r2.r1y2 && r1.r2y1 >= r2.r1y1) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } else if (r1.r2y1 > r2.r1y1 && r1.r2y2 < r2.r1y2 ) {
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } 
                    else if (r2.r1y2 > r1.r2y2 && r2.r1y1 <= r1.r2y2 && r2.r1y1 >= r1.r2y1) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } else if (r2.r1y1 >= r1.r2y1 && r2.r1y2 <= r1.r2y2) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    }
                }
                if (overlaps) {// output it to the reducer
                    // if mid point of the overlap region lies in the current cell area then compute the 
                    // join tuple otherwise forget
                    double midx = (oxl+oxr)/2;
                    double midy = (oyb+oyt)/2;
                    //int x = 1;
                    if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                    {
                        midx = midx+0.1; midy = midy+0.1;
                    }

                    if(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2)
                    {
                       // String output = r1 + "," + r2 +","+ joinType;
                        String output = r1.r1RowNum+ "," +r1.r2RowNum+ "," +r2.r1RowNum;
                        int key1= (cellRow * cellsPerRow) + cellCol;
                        context.write(new LongWritable(key1), new Text(output));   
                    }
                }
                else{
                //     System.out.println("-----------Reducer BC---No Overlap found-----------");
                }
            }
        }
    }
     @Override
    protected void cleanup(Context context) throws IOException{
        startr1 = System.currentTimeMillis() - startr1;
        System.out.println("-------------------------------------------");
        System.out.println("Seq R2 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1);
        System.out.println("-------------------------------------------");
    }
}