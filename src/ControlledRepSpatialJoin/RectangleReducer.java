package ControlledRepSpatialJoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class RectangleReducer extends Reducer<LongWritable, Rectangle, LongWritable, Text>
{
    long startr1;
    @Override
    protected void setup(Context context) throws IOException{
        startr1 = System.currentTimeMillis();
    }
    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> values, Context context) throws IOException, InterruptedException
    {
        ArrayList<Rectangle> la = new ArrayList();
        ArrayList<Rectangle> lb = new ArrayList();
        ArrayList<Rectangle> lc = new ArrayList();
        ArrayList<Rectangle> ld = new ArrayList();

        Configuration conf = context.getConfiguration();

        double cellWidth = conf.getDouble("cellWidth", 0.0D);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        int cellsPerCol = conf.getInt("p1NumOfReducersPerCol", 0);
        
        int cellNumber = (int)key.get();
        int cellRow = cellNumber / cellsPerRow;
        int cellCol = cellNumber % cellsPerRow;
        Rectangle cellRect = new Rectangle(0, 0, 0, cellCol * cellWidth, cellRow * cellWidth, (cellCol + 1) * cellWidth, (cellRow + 1) * cellWidth);
        for (Rectangle t : values)
        {
            Rectangle newt = new Rectangle(t);
            switch (newt.relationIndex) {
                case 1:
                    la.add(newt);
                    break;
                case 2:
                    lb.add(newt);
                    break;
                case 3:
                    lc.add(newt);
                    break;
                case 4:
                    ld.add(newt);
                    break;
                default:
                    break;
            }
        }
        joinABCD(cellWidth, cellNumber, cellRow, cellCol, cellsPerRow, cellsPerCol, la, lb, lc, ld, cellRect, context);
    }
    public void joinABCD(double cellWidth, int CellNumber, int cellRow, int cellCol, int cellsPerRow, int cellsPerCol, ArrayList<Rectangle> la, ArrayList<Rectangle> lb, ArrayList<Rectangle> lc, ArrayList<Rectangle> ld, Rectangle cellRect, Reducer<LongWritable, Rectangle, LongWritable, Text>.Context context) throws java.io.IOException, InterruptedException
    {
        boolean[] ra = new boolean[la.size()];
        boolean[] rb = new boolean[lb.size()];
        boolean[] rc = new boolean[lc.size()];
        boolean[] rd = new boolean[ld.size()];
        boolean crossed; boolean overlaps;
        for (int i = 0; i < la.size(); i++)
        {
            Rectangle r1 = (Rectangle)la.get(i);
            for (int j = 0; j < lb.size(); j++)
            {
                Rectangle r2 = (Rectangle)lb.get(j);
                overlaps = checkOverlap(r1, r2);
                if (overlaps)
                {
                    crossed = crossCellBoundary(r2, cellRect);
                    if (crossed)
                    {
                        ra[i] = true;
                        rb[j] = true;
                    }
                    for (int k = 0; k < lc.size(); k++)
                    {
                        Rectangle r3 = (Rectangle)lc.get(k);
                        overlaps = checkOverlap(r2, r3);
                        if (overlaps)
                        {
                            crossed = crossCellBoundary(r3, cellRect);
                            if (crossed) {
                                ra[i] = true;
                                rb[j] = true;
                                rc[k] = true;
                            }
                            for (int l = 0; l < ld.size(); l++)
                            {
                                Rectangle r4 = (Rectangle)ld.get(l);
                                overlaps = checkOverlap(r3, r4);
                                if (overlaps)
                                {
                                    double xx=checkRightMostRectangle(r1,r2,r3,r4);
                                    double yy=checkLowerMostRectangle(r1,r2,r3,r4);
                                    if(cellRect.x1<=xx && cellRect.x2>xx && cellRect.y1<=yy && cellRect.y2>yy)
                                    {   
                                     /*   int cr = 0;
                                        int key2= (cellRow * cellsPerRow) + cellCol;
                                        String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ cr;
                                        String r12 =  ","+r2.rowNum +","+ r2.relationIndex +","+ r2.x1 +","+ r2.y1 +","+ r2.x2 +","+ r2.y2 +","+ cr;
                                        String r13 =  ","+r3.rowNum +","+ r3.relationIndex +","+ r3.x1 +","+ r3.y1 +","+ r3.x2 +","+ r3.y2 +","+ cr;
                                        String r14 =  ","+r4.rowNum +","+ r4.relationIndex +","+ r4.x1 +","+ r4.y1 +","+ r4.x2 +","+ r4.y2 +","+ cr;
                                        context.write(new LongWritable(key2), new Text(r11));
                                        context.write(new LongWritable(key2), new Text(r12));
                                        context.write(new LongWritable(key2), new Text(r13));
                                        context.write(new LongWritable(key2), new Text(r14));   */
                                    }
                                    else{
                                        ra[i] = true;
                                        rb[j] = true;
                                        rc[k] = true;
                                        rd[l] = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        for (int i = 0; i < lb.size(); i++)
        {
            Rectangle r1 = lb.get(i);
            crossed = crossCellBoundary(r1, cellRect);
            if(crossed)
            {
                for (int j = 0; j < lc.size(); j++)
                {
                    Rectangle r2 = lc.get(j);
                    overlaps =checkOverlap(r1,r2);
                    if (overlaps)
                    {   //Check for B(r1) and C(r2) both are crossng the cell bounary if crosses then mark it(B,C) for replication
                        crossed = crossCellBoundary(r2, cellRect);
                        if(crossed)
                        {
                            rb[i] = true;
                            rc[j] = true;
                        }
                        for(int k=0; k<ld.size(); k++)
                        {
                            Rectangle r3=ld.get(k);
                            overlaps = checkOverlap(r2,r3);
                            if(overlaps)
                            { //Check for A(r1) is crossng the cell bounary if crosses then mark it(A,B, and C) for replication
                                rb[i] = true;
                                rc[j] = true;
                                rd[k] = true;
                            }   
                        }
                    }
                }
            }
        }
        for (int i = 0; i < lc.size(); i++)
        {
            Rectangle r1 = lc.get(i);
            crossed = crossCellBoundary(r1, cellRect);
            if(crossed)
            {   
                for (int j = 0; j < ld.size(); j++)
                {
                    Rectangle r2 = ld.get(j);
                    overlaps =checkOverlap(r1,r2);
                    if (overlaps)
                    {   //Check for C(r1) is crossng the cell bounary if crosses then mark it(C and D) for replication
                        rc[i] = true;
                        rd[j] = true;
                    }
                }
            }
        }
        int key1;
        key1= (cellRow * cellsPerRow) + cellCol;
        for(int i = 0;i<la.size();i++){
          //  if(ra[i] == true){
                Rectangle r1 = la.get(i);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ (ra[i]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
        //    }
        }
        for(int i = 0;i<lb.size();i++){
          //  if(rb[i] == true){
               Rectangle r1 = lb.get(i);
               String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ (rb[i]?1:0);
               context.write(new LongWritable(key1), new Text(r11));
          //  }   
        }
        for(int i = 0;i<lc.size();i++){
         //   if(rc[i] == true){
               Rectangle r1 = lc.get(i);
               String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ (rc[i]?1:0);
               context.write(new LongWritable(key1), new Text(r11));
         //   }
        }
        for(int i = 0;i<ld.size();i++){
         //   if(rd[i] == true){
               Rectangle r1 = ld.get(i);
               String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ (rd[i]?1:0);
               context.write(new LongWritable(key1), new Text(r11));
          //  }
        }
    }
    boolean checkOverlap(Rectangle r1, Rectangle r2) 
    {
        boolean overlaps=false;
          //Case 1:
        if (r1.x1 <= r2.x1 && r1.x2 >= r2.x1 && r1.x2 < r2.x2) {
            //                System.out.println("Case 1: LB ITEM" + r2.toString());

            if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                overlaps = true;
            } //Case
            else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
            } // Case : 
            else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                overlaps = true;
            } //Case :
            else if (r1.y2 < r2.y2 && r2.y1 < r1.y1) {
                overlaps = true;
            } 
        }//Case 2:    
        else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x1 && r2.x2< r1.x2)//changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                  //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
        {            //       System.out.println("Case 2: LB ITEM" + r2.toString());

            if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                overlaps = true;
            } 
            else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                overlaps = true;
            } 
            else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) {
                overlaps = true;
            }
        }   //case 3: 
        else if (r2.x1 < r1.x1 && r2.x2 > r1.x2) {
                    //        System.out.println("Case 3: LB ITEM" + r2.toString());
            if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                    overlaps = true;
            } 
            else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                    overlaps = true;
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 <= r1.y1) {//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                    overlaps = true;
            } 
            else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) {
                    overlaps = true;
            }
        }//Case 4:
        else if (r2.x1 > r1.x1 && r2.x2 < r1.x2) {
                  //         System.out.println("Case 4: LB ITEM" + r2.toString());
            if (r1.y1 >= r2.y1 && r1.y1 < r2.y2 && r2.y2 < r1.y2) {
                    overlaps = true;
            } 
            else if (r2.y1 <= r1.y1 && r1.y1 < r2.y2 && r1.y2 <= r2.y2) {
                    overlaps = true;
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r1.y2 <= r2.y2) { 
                    overlaps = true;
            } 
            else if (r1.y1 < r2.y1 && r1.y2 > r2.y1) {
                    overlaps = true;
            }
        }//Case 5:
        else if (r2.x1 == r1.x1 && r2.x2 == r1.x2) {
                  //         System.out.println("Case 5: LB ITEM" + r2.toString());
            if (r1.y2 > r2.y2 && r1.y1 <= r2.y2 && r1.y1 >= r2.y1) {
                    overlaps = true;
            } 
            else if (r1.y1 > r2.y1 && r1.y2 < r2.y2 ) {
                    overlaps = true;
            } 
            else if (r2.y2 > r1.y2 && r2.y1 <= r1.y2 && r2.y1 >= r1.y1) {
                    overlaps = true;
            } else if (r2.y1 >= r1.y1 && r2.y2 <= r1.y2) {
                    overlaps = true;
            }
        }
    return overlaps;
    }
    double checkRightMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3, Rectangle r4) 
    {
    //     System.out.println(r1.toString() +"\t"+ r2.toString()+"\t"+ r3.toString()+"\t"+r4.toString());
        double x=0;
        double p=(r1.x1 > r2.x1)?r1.x1:r2.x1;
        double q=(p > r3.x1)?p:r3.x1;
        double r=(q > r4.x1)?q:r4.x1;
    //    System.out.println("Largest X  \t"+r);
        if(r==r1.x1) {
            x=r1.x1;
        }
        else if(r==r2.x1) {
            x=r2.x1;
        }
        else if(r==r3.x1) {
            x=r3.x1;
        }
        else if(r==r4.x1) {
            x=r4.x1;
        }
//      System.out.println(x);
        return x;   // It will Return Right Most rectangle's  x1 point
    }
    double checkLowerMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3, Rectangle r4) 
    {
        double y=0;
        double p=(r1.y1 > r2.y1)?r1.y1:r2.y1;
        double q=(p > r3.y1)?p:r3.y1;
        double r=(q > r4.y1)?q:r4.y1;
  //      System.out.println("Largest Y  \t"+r);
        if(r==r1.y1) {
            y=r1.y1;
        }
        else if(r==r2.y1) {
            y=r2.y1;
        }
        else if(r==r3.y1) {
            y=r3.y1;
        }
        else if(r==r4.y1) {
            y=r4.y1;
        }   
        return y;   // It will Return Upper Most rectangle's  y1 point
    }
    boolean crossCellBoundary(Rectangle r, Rectangle cellRect) {
        if(((r.x1 >= cellRect.x1) && (r.y1 >= cellRect.y1)) && ((r.x2 >= cellRect.x2)  || (r.y2 >= cellRect.y2)))
        {
          return true;
        }
        else{
            return false;
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException{
        startr1 = System.currentTimeMillis() - startr1;
        System.out.println("-------------------------------------------");
        System.out.println("CRep R1 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1);
        System.out.println("-------------------------------------------");
    }
}