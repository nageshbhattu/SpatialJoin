package Sequential;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqCD extends Reducer<LongWritable, ABCJoinTuple, LongWritable, Text> 
{
    @Override
    public void reduce(LongWritable key, Iterable<ABCJoinTuple> values, Context context) throws IOException, InterruptedException 
    {
        
        ArrayList<ABCJoinTuple> lc = new ArrayList<>();
        ArrayList<ABCJoinTuple> ld = new ArrayList<>();
        
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        RectangleSeq cellRect = new RectangleSeq(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
     //   System.out.println("Reducer got ");
        
        for (ABCJoinTuple t : values) 
        {
            ABCJoinTuple newt = new ABCJoinTuple(t);
            //     System.out.println("ReducerBC----->>"+"\t"+ newt);
            //     System.out.println(t.r1RelationIndex+","+t.r2RelationIndex +","+t.r3RelationIndex);
            switch (t.abcType) {
                case 1:
                    if (t.r3RelationIndex == 3)
                    {
                        //          System.out.println("frm mapper ABC in RED CD^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+"\t"+newt );
                        lc.add(newt);
                    }   break;
                case 2:
                    if (t.r1RelationIndex == 4)
                    {
                        ld.add(newt);
                        //           System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+"\t"+newt );
                    }   break;
                default:
                    break;
            }
        }                                    
    //    System.out.println("Reducer CD**************************");

        join(cellNumber,lc, ld, 3, cellRect,context);
    }
    public void join(int CellNumber,ArrayList<ABCJoinTuple> lc, ArrayList<ABCJoinTuple> ld, int joinType, RectangleSeq cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < lc.size(); i++) 
        {
            ABCJoinTuple r1 = lc.get(i);
            for (int j = 0; j < ld.size(); j++) 
            {
                boolean overlaps = false;
                ABCJoinTuple r2 = ld.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
            //    System.out.println("CASE 1:Main ");
           //     System.out.println(r2.r3x1 +"<="+ r1.r1x1 +">"+r2.r3x2);
                //Case 1 : r1.x1 falls between r3.x1 and r3.x2
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
               if (r1.r3x1 <= r2.r1x1 && r1.r3x2 >= r2.r1x1 && r1.r3x2 <= r2.r1x2) { //ok
           
   
                    if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1 && r2.r1y2 >= r1.r3y2) { //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r1.r3x2;
                        oyt = r1.r3y2;
                    } // Case 2 
                    else if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1 && r2.r1y2 <= r1.r3y2) {  //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r1.r3x2;
                        oyt = r2.r1y2;
                    } // Case 3: 
                    else if (r1.r3y1 <= r2.r1y2 && r1.r3y2 >= r2.r1y2 && r2.r1y1 <= r1.r3y1) {  //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r3y1;
                        oxr = r1.r3x2;
                        oyt = r2.r1y2;
                    } //Case 4:
                    else if (r1.r3y2 < r2.r1y2 && r2.r1y1 < r1.r3y1) {   //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r3y1;
                        oxr = r1.r3x2;
                        oyt = r1.r3y2;
                    } // Case 2: r2.x1 is less than r1.x1
                }    
                else if (r2.r1x1 <= r1.r3x1 && r2.r1x2 >= r1.r3x1 && r2.r1x2< r1.r3x2)//ok //changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                          //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
                {
                    if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1 && r2.r1y2 >= r1.r3y2) {  //ok
                        
                        overlaps = true;//System.out.println("Case 2:1 LB ITEM" + r2.toString());
                        oxl = r1.r3x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r3y2;
                    } else if (r1.r3y1 <= r2.r1y1 && r1.r3y2 > r2.r1y1) {   //ok
       //                         System.out.println("Case 2:2 LB ITEM" + r2.toString());
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r3y1 <= r2.r1y2 && r1.r3y2 >= r2.r1y2 && r2.r1y1 <= r1.r3y1) {  //ok
                        overlaps = true;//System.out.println("Case 2:3 LB ITEM" + r2.toString());
                        oxl = r1.r3x1;
                        oyb = r1.r3y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r3y1 && r2.r1y2 >= r1.r3y2) {  //ok
                        overlaps = true;//System.out.println("Case 2:4 LB ITEM" + r2.toString());
                        oxl = r1.r3x1;
                        oyb = r1.r3y1;
                        oxr = r2.r1x2;
                        oyt = r1.r3y2;
                    }
                }   //case 3: 
                else if (r2.r1x1 <= r1.r3x1 && r2.r1x2 >= r1.r3x2) {   //ok
                  
                    if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1 && r2.r1y2 >=r1.r3y2) {  //ok
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r2.r1y1;
                        oxr = r1.r3x2;
                        oyt = r1.r3y2;
                    } else if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1 && r2.r1y2 <= r1.r3y2) {  //ok
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r2.r1y1;
                        oxr = r1.r3x2;
                        oyt = r2.r1y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r3y1 <= r2.r1y2 && r1.r3y2 >= r2.r1y2 && r2.r1y1 <= r1.r3y1) { //ok //I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r1.r3y1;
                        oxr = r1.r3x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r3y1 && r2.r1y2 >= r1.r3y1) {  //ok
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r1.r3y1;
                        oxr = r1.r3x2;
                        oyt = r1.r3y2;
                    }
                }//Case 4:
                else if (r2.r1x1 >= r1.r3x1 && r2.r1x2 <= r1.r3x2) {  //ok

                    if (r1.r3y1 >= r2.r1y1 && r1.r3y1 <= r2.r1y2 && r2.r1y2 <= r1.r3y2) {   //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r3y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r3y1 && r1.r3y1 <= r2.r1y2 && r1.r3y2 <= r2.r1y2) {  //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r3y1;
                        oxr = r2.r1x2;
                        oyt = r1.r3y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1 && r1.r3y2 <= r2.r1y2) { //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r3y2;
                    } else if (r1.r3y1 <= r2.r1y1 && r1.r3y2 >= r2.r1y1) {  //ok
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    }
                }//Case 5:
                else if (r2.r1x1 == r1.r3x1 && r2.r1x2 == r1.r3x2) {

                    if (r1.r3y2 >= r2.r1y2 && r1.r3y1 <= r2.r1y2 && r1.r3y1 >= r2.r1y1) {
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r1.r3y1;
                        oxr = r1.r3x2;
                        oyt = r2.r1y2;
                    } else if (r1.r3y1 >= r2.r1y1 && r1.r3y2 <= r2.r1y2 ) {
                        overlaps = true;
                        oxl = r1.r3x1;
                        oyb = r1.r3y1;
                        oxr = r1.r3x2;
                        oyt = r1.r3y2;
                    } 
                    else if (r2.r1y2 >= r1.r3y2 && r2.r1y1 <= r1.r3y2 && r2.r1y1 >= r1.r3y1) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r3y2;
                    } else if (r2.r1y1 >= r1.r3y1 && r2.r1y2 <= r1.r3y2) {
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    }
                }
                if (overlaps) 
                {
                     //   System.out.println("*******OverLAp Found***********");
                   //     System.out.println("oxl +\",\"+ oxr +\" +\"\\t\"+oyb+\",\"+\",\"+oyt"+oxl +","+ oxr +"\t"+oyb+","+","+oyt);
                 //       System.out.println("Reducer_BC\t"+ r1.toString() +"\t"+ r2.toString());
                        double midx = (oxl+oxr)/2;
                        double midy = (oyb+oyt)/2;
                        int x = 1;
                     //   System.out.println((midx+">="+cellRect.x1 +"&&"+ midx +"<="+cellRect.x2 +"&&"+ midy+">="+cellRect.y1 +"&&"+ midy+"<="+cellRect.y2));
                        if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                        {
                            midx = midx+0.1; midy = midy+0.1;
                        }
                        if(midx > cellRect.x1 && midx < cellRect.x2 && midy > cellRect.y1 && midy < cellRect.y2)
                        {
                          //  System.out.println("inside mid\t"+ r2.r3RowNum );
                            LongWritable key = new LongWritable(x);
                            String output = r1.r1RowNum+ "," +r1.r2RowNum+ "," +r1.r3RowNum+ "," +r2.r1RowNum;
                            Text Result = new Text(output);
                         //   System.out.println(Result);
                            context.write(key, Result);   
                        }
                }
                else{
                     //    System.out.println("*******OverLAp NOT Found***********");
                }
            }
        }
    }
    
     long startr3;
    @Override
    protected void setup(Context context) throws IOException{
        startr3 = System.currentTimeMillis();
    }
    @Override
    protected void cleanup(Context context) throws IOException{
        startr3 = System.currentTimeMillis() - startr3;
        System.out.println("-------------------------------------------");
        System.out.println("SEQ R3 IDr3 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr3 );
        System.out.println("-------------------------------------------");
    }
}