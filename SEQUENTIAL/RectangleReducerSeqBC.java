package Sequential;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqBC extends Reducer<LongWritable, ABJoinTuple, LongWritable, Text> 
{
    
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
        RectangleSeq cellRect = new RectangleSeq(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
       
       // System.out.println("Reducer got"+ values.);
        
        for (ABJoinTuple t : values) 
        {
            ABJoinTuple newt = new ABJoinTuple(t);
            switch (t.abType) {
                case 1:
                    if (t.r2RelationIndex == 2)
                    {
                        //            System.out.println("frm mapper AB in RED BC^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+"\t"+newt );
                        lb.add(newt);
                    }   break;
                case 2:
                    if (t.r1RelationIndex == 3)
                    {
                        //            System.out.println("frm mapper AB in RED BC^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+"\t"+newt );
                        lc.add(newt);
                    }   break;
                default:
                    break;
            }
        }
        join(cellNumber,lb, lc, 2, cellRect,context);
    }

    public void join(int CellNumber,ArrayList<ABJoinTuple> lb, ArrayList<ABJoinTuple> lc, int joinType, RectangleSeq cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < lb.size(); i++) 
        {
            ABJoinTuple r1 = lb.get(i);
            for (int j = 0; j < lc.size(); j++) 
            {
            //    System.out.println("case..");
                boolean overlaps = false;
                ABJoinTuple r2 = lc.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                 if (r1.r2x1 <= r2.r1x1 && r1.r2x2 >= r2.r1x1 && r1.r2x2 <= r2.r1x2) { //ok
            //       System.out.println("case1");
                    if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1 && r2.r1y2 >= r1.r2y2) { //ok
                        overlaps = true;
                //        System.out.println("case1.1");
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } // Case 1.2 
                    else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1 && r2.r1y2 <= r1.r2y2) {  //ok
                        overlaps = true;
               //         System.out.println("case1.2");
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } // Case 1.3: 
                    else if (r1.r2y1 <= r2.r1y2 && r1.r2y2 >= r2.r1y2 && r2.r1y1 <= r1.r2y1) {  //ok
                        overlaps = true;
               //         System.out.println("case1.3");
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } //Case 1.4:
                    else if (r1.r2y2 <= r2.r1y2 && r2.r1y1 <= r1.r2y1) {  //ok
                        overlaps = true;
                //        System.out.println("case1.4");
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } // Case 2: r2.x1 is less than r1.x1
                }    
                else if (r2.r1x1 <= r1.r2x1 && r2.r1x2 >= r1.r2x1 && r2.r1x2<= r1.r2x2) //ok//changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                         //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
                {
             //       System.out.println("case2");
                    if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1 && r2.r1y2 >= r1.r2y2) { //ok
             //           System.out.println("case2.1");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1) {  //ok
              //          System.out.println("case2.2");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r2y1 <= r2.r1y2 && r1.r2y2 >= r2.r1y2 && r2.r1y1 <= r1.r2y1) {  //ok
                        overlaps = true;
              //          System.out.println("case2.3");
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 >= r1.r2y2) {  //ok
                        overlaps = true;
            //            System.out.println("case2.4");
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    }
                }   //case 3: 
                else if (r2.r1x1 <= r1.r2x1 && r2.r1x2 >= r1.r2x2) {  //ok
             //       System.out.println("case3");
                    if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1 && r2.r1y2 >= r1.r2y2) { //ok
                        overlaps = true;
             //           System.out.println("case3.1");
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1 && r2.r1y2 <= r1.r2y2) {  //ok
                        overlaps = true;
               //         System.out.println("case3.2");
                        oxl = r1.r2x1;
                        oyb = r2.r1y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r2y1 <= r2.r1y2 && r1.r2y2 >= r2.r1y2 && r2.r1y1 <= r1.r2y1) {  //ok//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                        overlaps = true;
              //          System.out.println("case3.3");
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r2y1 && r2.r1y2 >= r1.r2y1) {  //ok
                        overlaps = true;
                //        System.out.println("case3.4");
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    }
                }//Case 4:
                else if (r2.r1x1 >= r1.r2x1 && r2.r1x2 <= r1.r2x2) {  //ok
            //        System.out.println("case4");
                    if (r1.r2y1 >= r2.r1y1 && r1.r2y1 <= r2.r1y2 && r2.r1y2 <= r1.r2y2) {  //ok
            //            System.out.println("case4.1");
                        overlaps = true;
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    } else if (r2.r1y1 <= r1.r2y1 && r1.r2y1 <= r2.r1y2 && r1.r2y2 <= r2.r1y2) {  //ok
                        overlaps = true;
               //         System.out.println("case4.2");
                        oxl = r2.r1x1;
                        oyb = r1.r2y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1 && r1.r2y2 <= r2.r1y2) { //ok
                        overlaps = true;
               //         System.out.println("case4.3");
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } else if (r1.r2y1 <= r2.r1y1 && r1.r2y2 >= r2.r1y1) {//ok
                        overlaps = true;
                //        System.out.println("case4.4");
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    }
                }//Case 5:
                else if (r2.r1x1 == r1.r2x1 && r2.r1x2 == r1.r2x2) {
            //        System.out.println("case5");
                    if (r1.r2y2 >= r2.r1y2 && r1.r2y1 <= r2.r1y2 && r1.r2y1 >= r2.r1y1) {
            //            System.out.println("case5.1");
                        overlaps = true;
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r2.r1y2;
                    } else if (r1.r2y1 >= r2.r1y1 && r1.r2y2 <= r2.r1y2 ) {
                        overlaps = true;
             //           System.out.println("case5.2");
                        oxl = r1.r2x1;
                        oyb = r1.r2y1;
                        oxr = r1.r2x2;
                        oyt = r1.r2y2;
                    } 
                    else if (r2.r1y2 >= r1.r2y2 && r2.r1y1 <= r1.r2y2 && r2.r1y1 >= r1.r2y1) {
                        overlaps = true;
             //           System.out.println("case5.3");
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r1.r2y2;
                    } else if (r2.r1y1 >= r1.r2y1 && r2.r1y2 <= r1.r2y2) {
                        overlaps = true;
                //        System.out.println("case5.4");
                        oxl = r2.r1x1;
                        oyb = r2.r1y1;
                        oxr = r2.r1x2;
                        oyt = r2.r1y2;
                    }
                }
                    if (overlaps) {
                        //if(r1.r2RelationIndex == 2)
                    //        System.out.println("Reducer_BC\t"+ r1.toString() +"\t"+ r2.toString());
// output it to the reducer
                        // if mid point of the overlap region lies in the current cell area then compute the 
                        // join tuple otherwise forget
                      //  System.out.println("\n\nr1.x1+r1.y1+r1.x2+r1.y2+r2.x1+r2.y1+r2.x2+r2.y2-->"+ r1.x1+","+r1.y1+",\t"+r1.x2+","+r1.y2+",\n"+r2.x1+","+r2.y1+",\t"+r2.x2+","+r2.y2);
                        double midx = (oxl+oxr)/2;
                        double midy = (oyb+oyt)/2;
                        int x = 1;
                        if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                        {
                            midx = midx+0.00001; midy = midy+0.00001;
                        }
                        
                        if(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2)
                        {
                            LongWritable key = new LongWritable(x);
                            String output = joinType + "," + r1 + "," + r2;
                            Text Result = new Text(output);
                            context.write(key, Result);   
                        }
                        
                    }
                    else{
                    //     System.out.println("-----------Reducer BC---No Overlap found-----------");
                    }
            }
        }
    }
    
     long startr2;
    @Override
    protected void setup(Context context) throws IOException{
        startr2 = System.currentTimeMillis();
    }
    @Override
    protected void cleanup(Context context) throws IOException{
        startr2 = System.currentTimeMillis() - startr2;
        System.out.println("-------------------------------------------");
        System.out.println("SEQ R2 IDr2 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr2 );
        System.out.println("-------------------------------------------");
    }
}