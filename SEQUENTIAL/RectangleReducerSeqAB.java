package Sequential;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducerSeqAB extends Reducer<LongWritable, RectangleSeq, LongWritable, Text> 
{
    @Override
    public void reduce(LongWritable key, Iterable<RectangleSeq> values, Context context) throws IOException, InterruptedException 
    {
        ArrayList<RectangleSeq> la = new ArrayList<>();
        ArrayList<RectangleSeq> lb = new ArrayList<>();
   
        Configuration conf = context.getConfiguration();
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        RectangleSeq cellRect = new RectangleSeq(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        //System.out.println("Reducer got ");
        for (RectangleSeq t : values) 
        {
            RectangleSeq newt = new RectangleSeq(t);
            //     System.out.println("ReducerAB -------T.Values>" + key +","+newt);
            switch (t.relationIndex) {
                case 1:
                    la.add(newt);
                    break;
                case 2:
                    lb.add(newt);
                    break;
                default:
                    break;
            }
        }
        join(cellNumber,la, lb, 1, cellRect,context);
    }
    public void join(int CellNumber,ArrayList<RectangleSeq> la, ArrayList<RectangleSeq> lb, int joinType, RectangleSeq cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < la.size(); i++) 
        {
            RectangleSeq r1 = la.get(i);
            for (int j = 0; j < lb.size(); j++) 
            {
                boolean overlaps = false;
                RectangleSeq r2 = lb.get(j);
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
            //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                if (r1.x1 <= r2.x1 && r1.x2 >= r2.x1 && r1.x2 <= r2.x2) { //ok
               
              
                    if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 >= r1.y2) { //ok
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    } // Case 2 
                    else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 <= r1.y2) { //ok
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } // Case 3: 
                    else if (r1.y1 <= r2.y2 && r1.y2 >= r2.y2 && r2.y1 <= r1.y1) {  //ok
                   
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } //Case 4:
                    else if (r1.y2 <= r2.y2 && r2.y1 <= r1.y1) {   //ok
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    } // Case 2: r2.x1 is less than r1.x1
                }    
                else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x1 && r2.x2<= r1.x2) //ok //changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                          //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
                {       
                    if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 >= r1.y2) {  //ok
                        
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r2.y1;
                        oxr = r2.x2;
                        oyt = r1.y2;
                    } else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1) {  //ok
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r2.y1;
                        oxr = r2.x2;
                        oyt = r2.y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.y1 <= r2.y2 && r1.y2 >= r2.y2 && r2.y1 <= r1.y1) { //ok
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r1.y1;
                        oxr = r2.x2;
                        oyt = r2.y2;
                    } else if (r2.y1 <= r1.y1 && r2.y2 >= r1.y2) { //ok
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r1.y1;
                        oxr = r2.x2;
                        oyt = r1.y2;
                    }
                }   //case 3: 
                else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x2) {  //ok
                    
                    if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 >= r1.y2) {   //ok
                       overlaps = true;
                        oxl = r1.x1;
                        oyb = r2.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    } else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 <= r1.y2) {  //ok
                       overlaps = true;
                        oxl = r1.x1;
                        oyb = r2.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.y1 <= r2.y2 && r1.y2 >= r2.y2 && r2.y1 <= r1.y1) {  //ok//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                    
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } else if (r2.y1 <= r1.y1 && r2.y2 >= r1.y1) {  //ok
                       overlaps = true;
                        oxl = r1.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    }
                }//Case 4:
                else if (r2.x1 >= r1.x1 && r2.x2 <= r1.x2) {  //ok
               
                    if (r1.y1 >= r2.y1 && r1.y1 <= r2.y2 && r2.y2 <= r1.y2) {  //ok
                       overlaps = true;
                        oxl = r2.x1;
                        oyb = r1.y1;
                        oxr = r2.x2;
                        oyt = r2.y2;
                    } else if (r2.y1 <= r1.y1 && r1.y1 <= r2.y2 && r1.y2 <= r2.y2) {  //ok
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r1.y1;
                        oxr = r2.x2;
                        oyt = r1.y2;
                    } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                    else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r1.y2 <= r2.y2) { //ok
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r2.x2;
                        oyt = r1.y2;
                    } else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1) {  //ok
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r2.x2;
                        oyt = r2.y2;
                    }
                }//Case 5:
                else if (r2.x1 == r1.x1 && r2.x2 == r1.x2) {
                
                    if (r1.y2 >= r2.y2 && r1.y1 <= r2.y2 && r1.y1 >= r2.y1) {
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } else if (r1.y1 >= r2.y1 && r1.y2 <= r2.y2 ) {
                        overlaps = true;
                        oxl = r1.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    } 
                    else if (r2.y2 >= r1.y2 && r2.y1 <= r1.y2 && r2.y1 >= r1.y1) {
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r2.x2;
                        oyt = r1.y2;
                    } else if (r2.y1 >= r1.y1 && r2.y2 <= r1.y2) {
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r2.x2;
                        oyt = r2.y2;
                    }
                }
                    if (overlaps) {// output it to the reducer
                //        System.out.println("Reducer_AB\t"+ r1.toString() +"\t"+ r2.toString());
                        double midx = (oxl+oxr)/2;
                        double midy = (oyb+oyt)/2;
                        int x = 1;
                        if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                        {
                            midx = midx+0.1; midy = midy+0.1;
                        }
                        if(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2)
                        {
                            LongWritable key = new LongWritable(x);
                            String output = joinType + "," + r1 + "," + r2;
                    //        System.out.println("\n\nif output is-->"+"\t"+output);
                            Text Result = new Text(output);
                            context.write(key, Result);   
                        }
                    }
                }
        }
    }
     long startr1,reducerTaskId;
    @Override
    protected void setup(Context context) throws IOException{
        startr1 = System.currentTimeMillis();
    }
    @Override
    protected void cleanup(Context context) throws IOException{
        startr1 = System.currentTimeMillis() - startr1;
        System.out.println("-------------------------------------------");
        System.out.println("SEQ R1 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1 );
        System.out.println("-------------------------------------------");
    }
}