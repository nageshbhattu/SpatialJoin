package EMSpatialJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RectangleReducer extends Reducer<LongWritable, Rectangle, LongWritable, Text> {

    MultipleOutputs<LongWritable, Text> mos;
     @Override
     public void setup(Context context) {
          mos = new MultipleOutputs(context);
    }  

    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> values, Context context) throws IOException, InterruptedException {
        ArrayList<Rectangle> la = new ArrayList<>();
        ArrayList<Rectangle> lb = new ArrayList<>();
        ArrayList<Rectangle> lc = new ArrayList<>();
        ArrayList<Rectangle> ld = new ArrayList<>();
      //  int i, j;
        Configuration conf = context.getConfiguration();
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        String multiple = conf.get("multiple");
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        for (Rectangle t : values) {
            Rectangle newt = new Rectangle(t);
         //  System.out.println("Reducer\t"+key+" \t"+ newt);
            if (t.relationIndex == 1) {
                la.add(newt);
            } else if (t.relationIndex == 2) {
                lb.add(newt);
            } else if (t.relationIndex == 3) {
                lc.add(newt);
            } else if (t.relationIndex == 4) {
                ld.add(newt);
            } else {
            }
        }
    //    System.out.println(la.size()  + ":: " + lb.size()+ ":: " + lc.size() + ":: " + ld.size());
        join(multiple, cellNumber,la, lb, 1, cellRect,context);
        join(multiple, cellNumber,lb, lc, 2, cellRect, context);
        join(multiple, cellNumber,lc, ld, 3, cellRect, context);
        
    }
    public void join(String multiple, int CellNumber,ArrayList<Rectangle> la, ArrayList<Rectangle> lb, int joinType, Rectangle cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        for (int i = 0; i < la.size(); i++) {
            Rectangle r1 = la.get(i);
        //    System.out.println(" LA ITEM" + r1.toString());
            for (int j = 0; j < lb.size(); j++) {
                Rectangle r2 = lb.get(j);
        //      System.out.println(" LB ITEM" + r2.toString());
                boolean overlaps = false;
                double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
                //Case 1 : r2.x1 falls between r1.x1 and r1.x2
                if (r1.x1 <= r2.x1 && r1.x2 >= r2.x1 && r1.x2 < r2.x2) {
                            //       System.out.println("Case 1: LB ITEM" + r2.toString());

                    if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                    //    System.out.println("Case 1:1 LB ITEM" + r2.toString());
                        overlaps = true;
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    } // Case 2 
                    else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                        overlaps = true;//System.out.println("Case 1:2 LB ITEM" + r2.toString());
                        oxl = r2.x1;
                        oyb = r2.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } // Case 3: 
                    else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                        overlaps = true;//System.out.println("Case 1:3 LB ITEM" + r2.toString());
                        oxl = r2.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r2.y2;
                    } //Case 4:
                    else if (r1.y2 < r2.y2 && r2.y1 < r1.y1) {
                        overlaps = true;//System.out.println("Case 1:4 LB ITEM" + r2.toString());
                        oxl = r2.x1;
                        oyb = r1.y1;
                        oxr = r1.x2;
                        oyt = r1.y2;
                    } // Case 2: r2.x1 is less than r1.x1
                }    
                else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x1 && r2.x2< r1.x2)//changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                         //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
                {               //   System.out.println("Case 2: LB ITEM" + r2.toString());

                        if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                            overlaps = true;//System.out.println("Case 2:1 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r2.y1;
                            oxr = r2.x2;
                            oyt = r1.y2;
                        } else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                            
                            overlaps = true;//System.out.println("Case 2:2 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r2.y1;
                            oxr = r2.x2;
                            oyt = r2.y2;
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                            overlaps = true;//System.out.println("Case 2:3 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r1.y1;
                            oxr = r2.x2;
                            oyt = r2.y2;
                        } else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) {
                            overlaps = true;//System.out.println("Case 2:4 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r1.y1;
                            oxr = r2.x2;
                            oyt = r1.y2;
                        }
                }   //case 3: 
                else if (r2.x1 < r1.x1 && r2.x2 > r1.x2) {
                                  //  System.out.println("Case 3: LB ITEM" + r2.toString());

                        if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                            overlaps = true;//System.out.println("Case 3:1 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r2.y1;
                            oxr = r1.x2;
                            oyt = r1.y2;
                        } else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                            overlaps = true;//System.out.println("Case 3:2 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r2.y1;
                            oxr = r1.x2;
                            oyt = r2.y2;
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 <= r1.y1) {//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                            overlaps = true;//System.out.println("Case 3:3 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r1.y1;
                            oxr = r1.x2;
                            oyt = r2.y2;
                        } else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) {
                            overlaps = true;//System.out.println("Case 3:4 LB ITEM" + r2.toString());
                            oxl = r1.x1;
                            oyb = r1.y1;
                            oxr = r1.x2;
                            oyt = r1.y2;
                        }
                    }//Case 4:
                    else if (r2.x1 > r1.x1 && r2.x2 < r1.x2) {
                                 // System.out.println("Case 4: LB ITEM" + r2.toString());

                        if (r1.y1 >= r2.y1 && r1.y1 < r2.y2 && r2.y2 < r1.y2) {
                            overlaps = true;
                            oxl = r2.x1;
                            oyb = r1.y1;
                            oxr = r2.x2;
                            oyt = r2.y2;
                        } else if (r2.y1 <= r1.y1 && r1.y1 < r2.y2 && r1.y2 <= r2.y2) {
                            overlaps = true;
                            oxl = r2.x1;
                            oyb = r1.y1;
                            oxr = r2.x2;
                            oyt = r1.y2;
                        } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
                        else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r1.y2 <= r2.y2) { 
                            overlaps = true;
                            oxl = r2.x1;
                            oyb = r2.y1;
                            oxr = r2.x2;
                            oyt = r1.y2;
                        } else if (r1.y1 < r2.y1 && r1.y2 > r2.y1) {
                            overlaps = true;
                            oxl = r2.x1;
                            oyb = r2.y1;
                            oxr = r2.x2;
                            oyt = r2.y2;
                        }
                    }//Case 5:
                    else if (r2.x1 == r1.x1 && r2.x2 == r1.x2) {
                                 // System.out.println("Case 5: LB ITEM" + r2.toString());

                        if (r1.y2 > r2.y2 && r1.y1 <= r2.y2 && r1.y1 >= r2.y1) {
                            overlaps = true;
                            oxl = r1.x1;
                            oyb = r1.y1;
                            oxr = r1.x2;
                            oyt = r2.y2;
                        } else if (r1.y1 > r2.y1 && r1.y2 < r2.y2 ) {
                            overlaps = true;
                            oxl = r1.x1;
                            oyb = r1.y1;
                            oxr = r1.x2;
                            oyt = r1.y2;
                        } 
                        else if (r2.y2 > r1.y2 && r2.y1 <= r1.y2 && r2.y1 >= r1.y1) {
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
                      //  System.out.println("oxl\t"+ oxl +"\toxr"+oxr+"\toyb"+ oyb +"\toyt"+oyt);

                        double midx = (oxl+oxr)/2;
                        double midy = (oyb+oyt)/2;
                      //  System.out.println("MidX\t"+ midx +"::"+midy);
                        if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
                        {
                            midx = midx+0.1; midy = midy+0.1;
                        }
                   //   System.out.println(" Inside verla ");
                        int x = 1;
                  //      System.out.println("MidX\t"+ midx +"::"+midy);
                  //      System.out.println(midx+">"+cellRect.x1 +"&&"+ midx +"<"+cellRect.x2 +"&&"+ midy+">"+cellRect.y1 +"&&"+ midy+"<"+cellRect.y2);
                        
                        if(midx>cellRect.x1 && midx <cellRect.x2 && midy>cellRect.y1 && midy<cellRect.y2){
                            LongWritable key = new LongWritable(x);
                            String output = joinType + "," + r1 + "," + r2;
                            Text Result = new Text(output);
                          //  System.out.println(" Insie mid point " );
                           // context.write(key, Result);   
                           //mos.write(multiple, key, Result, multiple);
                           mos.write(key, Result, multiple);
                        }
                    }
                
            }
            /*********/
        }
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }   
    
}