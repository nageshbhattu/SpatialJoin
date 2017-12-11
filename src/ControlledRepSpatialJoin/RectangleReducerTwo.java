package ControlledRepSpatialJoin;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RectangleReducerTwo extends Reducer<LongWritable,Rectangle,LongWritable,Text> 
{
    long startr2;
    @Override
    protected void setup(Context context) throws IOException{
        startr2 = System.currentTimeMillis();
    }
    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> value,Context context) throws IOException, InterruptedException 
    {
        TreeSet<Rectangle> la = new TreeSet<>();
        TreeSet<Rectangle> lb = new TreeSet<>();
        TreeSet<Rectangle> lc = new TreeSet<>();
        TreeSet<Rectangle> ld = new TreeSet<>();
  
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        for(Rectangle t:value)
        {
            Rectangle newt = new Rectangle(t);
            switch (t.relationIndex) {
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
        join(cellNumber, cellRow, cellCol, cellsPerRow, la, lb, lc, ld, cellRect,context);
    }
    public void join(int CellNumber, int cellRow, int cellCol, int cellsPerRow, TreeSet<Rectangle> la, TreeSet<Rectangle> lb, TreeSet<Rectangle> lc, TreeSet<Rectangle> ld, Rectangle cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        boolean overlaps;
        int key1;
        Iterator<Rectangle> itra=la.iterator();
        while(itra.hasNext()){
            Rectangle r1 = itra.next();
            Iterator<Rectangle> itrb=lb.iterator();
            while(itrb.hasNext()){
                Rectangle r2 = itrb.next();
                overlaps = checkOverlap(r1,r2);
                if(overlaps){
                    Iterator<Rectangle> itrc=lc.iterator();
                    while(itrc.hasNext()){
                        Rectangle r3 = itrc.next();
                        overlaps = checkOverlap(r2,r3);
                        if(overlaps){
                            Iterator<Rectangle> itrd=ld.iterator();
                            while(itrd.hasNext()){
                                Rectangle r4 = itrd.next();
                                overlaps = checkOverlap(r3,r4);
                                if(overlaps){
                                    double xx=checkRightMostRectangle(r1,r2,r3,r4);
                                    double yy=checkLowerMostRectangle(r1,r2,r3,r4);
                                    key1= (cellRow * cellsPerRow) + cellCol;
                                    String output = key1 + "\t" + r1.rowNum +","+ r2.rowNum +","+ r3.rowNum + "," + r4.rowNum ;
                                    Text Result = new Text(output);
                                    if((cellRect.x1 <= xx) && (cellRect.x2 > xx) && (cellRect.y1 <= yy) && (cellRect.y2 > yy))
                                    {
                              //         System.out.println("Reducertwo------noCross-----\t"+ output);
                                       context.write(new LongWritable(key1), Result);
                                    }
                               }
                           }
                       }
                   }
                }
            }
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
    @Override
    protected void cleanup(Context context) throws IOException{
        startr2 = System.currentTimeMillis() - startr2;
        System.out.println("-------------------------------------------");
        System.out.println("CRep R2 IDr2 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr2);
        System.out.println("-------------------------------------------");
    }
}