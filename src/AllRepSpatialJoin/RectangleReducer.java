package AllRepSpatialJoin;

import java.io.IOException;
import static java.lang.reflect.Array.set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RectangleReducer extends Reducer<LongWritable,Rectangle,LongWritable,Text> 
{
    MultipleOutputs<LongWritable, Text> mos;
     @Override
     public void setup(Context context) {
          mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> value,Context context) throws IOException, InterruptedException 
    {
        ArrayList<Rectangle> la = new ArrayList<>();
        ArrayList<Rectangle> lb = new ArrayList<>();
        ArrayList<Rectangle> lc = new ArrayList<>();
        ArrayList<Rectangle> ld = new ArrayList<>();
        
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        String allRep = conf.get("allRep");
       // String abc = conf.get(allRep);
        
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        for(Rectangle t:value)
        {
            Rectangle newt = new Rectangle(t);
       //     System.out.println("***************\t"+newt);
            if (t.relationIndex == 1) {
                la.add(newt);
            } else if (t.relationIndex == 2) {
                lb.add(newt);
            } else if (t.relationIndex == 3) {
                lc.add(newt);
            } else if (t.relationIndex == 4) {
                ld.add(newt);
            } 
            else {
            }
        }
        Set<Rectangle> toRetain = new TreeSet<>();
        toRetain.addAll(la);
        Set<Rectangle> set = new LinkedHashSet<>(la);
        set.retainAll(new LinkedHashSet<>(toRetain));
        la = new ArrayList<>(set);

       // Set<Rectangle> toRetain1 = new TreeSet<Rectangle>();
        toRetain.addAll(lb);
        Set<Rectangle> set1 = new LinkedHashSet<>(lb);
        set1.retainAll(new LinkedHashSet<>(toRetain));
        lb = new ArrayList<>(set1);

        toRetain.addAll(lc);
        Set<Rectangle> set2 = new LinkedHashSet<>(lc);
        set2.retainAll(new LinkedHashSet<>(toRetain));
        lc = new ArrayList<>(set2);

        toRetain.addAll(ld);
        Set<Rectangle> set3 = new LinkedHashSet<>(ld);
        set3.retainAll(new LinkedHashSet<>(toRetain));
        ld = new ArrayList<>(set3);
    
      
        join(allRep, cellNumber, cellRow, cellCol, cellsPerRow, la, lb, lc, ld, 1, 2, 3, cellRect,context);
    }
  
    public void join(String allRep, int CellNumber, int cellRow, int cellCol, int cellsPerRow, ArrayList<Rectangle> la, ArrayList<Rectangle> lb, ArrayList<Rectangle> lc, ArrayList<Rectangle> ld, int joinType1, int joinType2, int joinType3, Rectangle cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        boolean overlaps;
        double x, y;
        int key1=0;
        for (int i = 0; i < la.size(); i++) 
        {
            Rectangle r1 = la.get(i);
            for (int j = 0; j < lb.size(); j++) 
            {
                Rectangle r2 = lb.get(j);
                overlaps = checkOverlap(r1,r2);
                if (overlaps) 
                {
                  //  System.out.println("-------------Overlap found b/n A and B-----------");
                    for (int k = 0; k < lc.size(); k++) 
                    {
                        Rectangle r3 = lc.get(k);
                        overlaps = checkOverlap(r2,r3);
                        if (overlaps) 
                        {
                            for (int l = 0; l < ld.size(); l++) 
                            {
                                Rectangle r4 = ld.get(l);
                                overlaps = checkOverlap(r3,r4);
                                if (overlaps) 
                                {
                    //                System.out.println("-------------Overlap found b/n A, B, C and D-----------");
                                    double xx=checkRightMostRectangle(r1,r2,r3,r4);
                                    double yy=checkLowerMostRectangle(r1,r2,r3,r4);
                              //      System.out.println("inside XX   and YY>\t"+xx+"::"+yy);
                                    String output = r1.rowNum +","+ r2.rowNum +","+ r3.rowNum + "," + r4.rowNum ;
                                    Text Result = new Text(output);
                                            
                                    key1= (cellRow * cellsPerRow) + cellCol;
                               //   System.out.println((cellRect.x1 +"< "+ xx) +" && "+  (cellRect.x2 +">"+  xx) +"&&"+  (cellRect.y1 +"< "+  yy) +"&& "+  (cellRect.y2 +"> "+  yy));    
                                    if((cellRect.x1 < xx) && (cellRect.x2 > xx) && (cellRect.y1 < yy) && (cellRect.y2 > yy))
                                    {
                                         //       System.out.println("Insie Cellrect---->\t");
                                        context.write(new LongWritable(key1), Result);
                                        mos.write(allRep, new LongWritable(key1), Result, "all");
                                    }
                                }
                                else 
                                {
                               //     System.out.println(" Overlap not found b/n C and D-----------");
                                }
                            }
                        }
                        else
                        {
                          // System.out.println(" Overlap not found b/n B and C-----------");
                        }
                    }
                }
                else
                {
                 //   System.out.println("-------------No Overlap found b/n A and B-----------");
                }
            }
        }
        
    }
    
    boolean checkOverlap(Rectangle r1, Rectangle r2) 
    {
        boolean overlaps=false;
                //Case 1:
                if (r1.x1 <= r2.x1 && r1.x2 >= r2.x1 && r1.x2 < r2.x2) {
                       //             System.out.println("Case 1: LB ITEM" + r2.toString());
                    
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
                {      //             System.out.println("Case 2: LB ITEM" + r2.toString());

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
                           //         System.out.println("Case 3: LB ITEM" + r2.toString());
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
                            //       System.out.println("Case 4: LB ITEM" + r2.toString());
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
                           //        System.out.println("Case 5: LB ITEM" + r2.toString());
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
        double p=(r1.x2 > r2.x2)?r1.x2:r2.x2;
        double q=(p > r3.x2)?p:r3.x2;
        double r=(q > r4.x2)?q:r4.x2;
    //    System.out.println("Largest X  \t"+r);
        if(r==r1.x2) {
            x=r1.x2;
        }
        else if(r==r2.x2) {
            x=r2.x2;
        }
        else if(r==r3.x2) {
            x=r3.x2;
        }
        else if(r==r4.x2) {
            x=r4.x2;
        }
  //      System.out.println(x);
        return x;   // It will Return Right Most rectangle's  x1 point
    }
    double checkLowerMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3, Rectangle r4) 
    {
        double y=0;
        double p=(r1.y2 > r2.y2)?r1.y2:r2.y2;
        double q=(p > r3.y2)?p:r3.y2;
        double r=(q > r4.y2)?q:r4.y2;
  //      System.out.println("Largest Y  \t"+r);
        if(r==r1.y2) {
            y=r1.y2;
        }
        else if(r==r2.y2) {
            y=r2.y2;
        }
        else if(r==r3.y2) {
            y=r3.y2;
        }
        else if(r==r4.y2) {
            y=r4.y2;
        }   
        return y;   // It will Return Upper Most rectangle's starting y1 point
    }
    @Override
       protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}