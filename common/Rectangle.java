package Common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Rectangle  implements Writable, Comparable<Rectangle>
{
    // Coordinates of the rectangle
    public double x1,y1,x2,y2;
    public int relationIndex;
    public int rowNum;
    public int count;
    //int crossed;
    /**
     * Used in sorting the data in the reducer
     */
    public Rectangle() {

    }
    public Rectangle(int rowNum, int relIndex,double x1, double y1, double x2,double y2) {
        this.rowNum = rowNum;
        this.relationIndex=relIndex;
        this.x1= x1;
        this.y1= y1;
        this.x2 = x2;
        this.y2 = y2;
    }
    
     public Rectangle(int rowNum, int relIndex,double x1, double y1, double x2,double y2, int count) {
        this.rowNum = rowNum;
        this.relationIndex=relIndex;
        this.x1= x1;
        this.y1= y1;
        this.x2 = x2;
        this.y2 = y2;
        this.count =count;
    }
   
    public Rectangle(Rectangle r) {
        this.rowNum = r.rowNum;
        this.relationIndex = r.relationIndex;
        this.x1 = r.x1;
        this.x2 = r.x2;
        this.y1 = r.y1;
        this.y2 = r.y2;
    }
    @Override
    public int compareTo(Rectangle arg0) {
        // TODO Auto-generated method stub
        if(relationIndex<arg0.relationIndex)
            return -1;
        else if(relationIndex>arg0.relationIndex)
            return 1;
        else 
            return rowNum-arg0.rowNum;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        rowNum = in.readInt();
        relationIndex = in.readInt();
        x1 = in.readDouble();
        y1 = in.readDouble();
        x2 = in.readDouble();
        y2 = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(rowNum);
        out.writeInt(relationIndex);
        out.writeDouble(x1);
        out.writeDouble(y1);
        out.writeDouble(x2);
        out.writeDouble(y2);
    }
    @Override
    public String toString() {
        return  rowNum + "," +relationIndex+"," + x1 + ","+y1 + "," + x2 + ","+y2;
    }
    public static boolean crossCellBoundary(Rectangle r, Rectangle cellRect) {
       // if(r.rowNum==2802833|| r.rowNum==5420487){boolean fl = (r.x1 <= cellRect.x1) || (r.y1 <= cellRect.y1) || (r.x2 >= cellRect.x2)  || (r.y2 >= cellRect.y2);
         //   System.out.println("CellBoundarycross" + fl + "\t"+ r.toString());
           // System.out.println("cellrectttt\t"+cellRect.toString());
        //}
        return (r.x1 <= cellRect.x1) || (r.y1 <= cellRect.y1) || (r.x2 >= cellRect.x2)  || (r.y2 >= cellRect.y2);
    }
    public static boolean project(Rectangle r, Rectangle cellRect){
        return (cellRect.x1<= r.x1 && r.x1 <cellRect.x2 &&   cellRect.y1<= r.y1 && r.y1 <cellRect.y2);
    }
    public static boolean inside(double xx, double yy, Rectangle cellRect){
        return (cellRect.x1<= xx && xx <cellRect.x2 &&   cellRect.y1<= yy && yy <cellRect.y2);
    }
    public static boolean checkOverlapWithMidPoint(Rectangle r1, Rectangle r2, Rectangle cellRect) 
    {
        boolean overlaps=false; boolean inside = false;
        double oxl = 0, oxr = 0, oyb = 0, oyt = 0;
        //Case 1 : r2.x1 falls between r1.x1 and r1.x2
        if (r1.x1 <= r2.x1 && r1.x2 >= r2.x1 && r1.x2 < r2.x2) {
            
            if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                overlaps = true;
                oxl = r2.x1;
                oyb = r2.y1;
                oxr = r1.x2;
                oyt = r1.y2;
            } // Case 2 
            else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
                oxl = r2.x1;
                oyb = r2.y1;
                oxr = r1.x2;
                oyt = r2.y2;
            } // Case 3: 
            else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                overlaps = true;
                oxl = r2.x1;
                oyb = r1.y1;
                oxr = r1.x2;
                oyt = r2.y2;
            } //Case 4:
            else if (r1.y2 <= r2.y2 && r2.y1 < r1.y1) {
                overlaps = true;
                oxl = r2.x1;
                oyb = r1.y1;
                oxr = r1.x2;
                oyt = r1.y2;
            } // Case 2: r2.x1 is less than r1.x1
        }    
        else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x1 && r2.x2< r1.x2)//changed from (r1.x1 <= r2.x2 && r1.x2 > r2.x2 ) to 
                                                                  //(r2.x1 <= r1.x1 && r2.x2 > r1.x1 && r2.x2<= r1.x2)
        {       
            if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r2.y1;
                oxr = r2.x2;
                oyt = r1.y2;
            } else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r2.y1;
                oxr = r2.x2;
                oyt = r2.y2;
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 < r1.y1) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r1.y1;
                oxr = r2.x2;
                oyt = r2.y2;
            } else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r1.y1;
                oxr = r2.x2;
                oyt = r1.y2;
            }
        }   //case 3: 
        else if (r2.x1 < r1.x1 && r2.x2 > r1.x2) {

            if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 > r1.y2) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r2.y1;
                oxr = r1.x2;
                oyt = r1.y2;
            } else if (r1.y1 <= r2.y1 && r1.y2 > r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r2.y1;
                oxr = r1.x2;
                oyt = r2.y2;
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y2 && r1.y2 > r2.y2 && r2.y1 <= r1.y1) {//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                overlaps = true;
                oxl = r1.x1;
                oyb = r1.y1;
                oxr = r1.x2;
                oyt = r2.y2;
            } else if (r2.y1 < r1.y1 && r2.y2 > r1.y1) {
                overlaps = true;
                oxl = r1.x1;
                oyb = r1.y1;
                oxr = r1.x2;
                oyt = r1.y2;
            }
        }//Case 4:
        else if (r2.x1 > r1.x1 && r2.x2 < r1.x2) {

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
            double midx = (oxl+oxr)/2;
            double midy = (oyb+oyt)/2;
           // int x = 1;
            if((midx == cellRect.x1) || (midx == cellRect.x2) || (midy == cellRect.y1) || (midy<cellRect.y2))
            {
                midx = midx+0.1; midy = midy+0.1;
            }
            if(midx>=cellRect.x1 && midx <cellRect.x2 && midy>=cellRect.y1 && midy<cellRect.y2)
            {
               inside = true;   
            }
        }
        return inside;
    }
    public static boolean checkOverlap(Rectangle r1, Rectangle r2) 
    {
        boolean overlaps=false;
        //Case 1:
    /*    if(r1.relationIndex == 2 && r2.relationIndex == 1)
            System.out.println("CaseAB");
        if(r1.relationIndex == 2 && r2.relationIndex == 3)
            System.out.println("CaseBC");
        if(r1.relationIndex == 3 && r2.relationIndex == 4)
            System.out.println("CaseCD");                       */
        if (r1.x1 <= r2.x1 && r1.x2 >= r2.x1 && r1.x2 <= r2.x2) {
            
        //    System.out.println("Case1:\t"+r1.relationIndex+","+ r2.relationIndex);
            if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 >= r1.y2) {
                overlaps = true;
          //      System.out.println("Case1.1:\t"+r1.relationIndex+","+ r2.relationIndex);
            } //Case
            else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
            //    System.out.println("Case1.2:\t"+r1.relationIndex+","+ r2.relationIndex);
            } // Case : 
            else if (r1.y1 <= r2.y2 && r1.y2 >= r2.y2 && r2.y1 <= r1.y1) {
                overlaps = true;
        //     System.out.println("Case1.3:\t"+r1.relationIndex+","+ r2.relationIndex);
            } //Case :
            else if (r1.y2 <= r2.y2 && r2.y1 <= r1.y1) {
                overlaps = true;
       //    System.out.println("Case1.4:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
        }//Case 2:    
        else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x1 && r2.x2<= r1.x2){
       //  System.out.println("Case2:\t"+r1.relationIndex+","+ r2.relationIndex);
           // System.out.println("Case2:\t"+r1.relationIndex+","+ r2.relationIndex+"\t"+r1.y1 +"<="+ r2.y1 +"&&"+ r1.y2 +">="+ r2.y1 +"&&"+ r2.y2 +">="+ r1.y2);
            if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 >= r1.y2) {
                overlaps = true;
         //     System.out.println("Case2.1:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            //else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 <= r1.y2) {
            else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1) {
                    overlaps = true;
           // System.out.println("Case2.2:\t"+r1.relationIndex+","+ r2.relationIndex);
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y2 && r1.y2 >= r2.y2 && r2.y1 <= r1.y1) {
                overlaps = true;
         //    System.out.println("Case2.3:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r2.y1 <= r1.y1 && r2.y2 >= r1.y2) {
                overlaps = true;
          //      System.out.println("Case2.4:\t"+r1.relationIndex+","+ r2.relationIndex);
            }
        }   //case 3: 
        else if (r2.x1 <= r1.x1 && r2.x2 >= r1.x2) {
        //    System.out.println("Case3:\t"+r1.relationIndex+","+ r2.relationIndex);
            if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 >= r1.y2) {
                overlaps = true;
        //        System.out.println("Case3.1:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
          //     System.out.println("Case3.2:\t"+r1.relationIndex+","+ r2.relationIndex);
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y2 && r1.y2 >= r2.y2 && r2.y1 <= r1.y1) {//I changed from r2.y1 < r1.y1  to r2.y1 <= r1.y1
                overlaps = true;
            /// System.out.println("Case3.3:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r2.y1 <= r1.y1 && r2.y2 >= r1.y1) {
                overlaps = true;
             //  System.out.println("Case3.4:\t"+r1.relationIndex+","+ r2.relationIndex);
            }
        }//Case 4:
        else if (r2.x1 >= r1.x1 && r2.x2 <= r1.x2) {
      //      System.out.println("Case4:\t"+r1.relationIndex+","+ r2.relationIndex);
            if (r1.y1 >= r2.y1 && r1.y1 <= r2.y2 && r2.y2 <= r1.y2) {
                overlaps = true;
        //     System.out.println("Case4.1:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r2.y1 <= r1.y1 && r1.y1 <= r2.y2 && r1.y2 <= r2.y2) {
                overlaps = true;
        //    System.out.println("Case4.2:\t"+r1.relationIndex+","+ r2.relationIndex);
            } // Case 2b: r2.y2 falls between r1.y1 and r1.y2
            else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1 && r1.y2 <= r2.y2) { 
                overlaps = true;
        //    System.out.println("Case4.3:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r1.y1 <= r2.y1 && r1.y2 >= r2.y1) {
                overlaps = true;
          //  System.out.println("Case4.4:\t"+r1.relationIndex+","+ r2.relationIndex);
            }
        }//Case 5:
        else if (r2.x1 == r1.x1 && r2.x2 == r1.x2) {
        //    System.out.println("Case5:\t"+r1.relationIndex+","+ r2.relationIndex);
            if (r1.y2 >= r2.y2 && r1.y1 <= r2.y2 && r1.y1 >= r2.y1) {
                overlaps = true;
          //      System.out.println("Case5.1:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r1.y1 >= r2.y1 && r1.y2 <= r2.y2 ) {
                overlaps = true;
        //        System.out.println("Case5.2:\t"+r1.relationIndex+","+ r2.relationIndex);
            } 
            else if (r2.y2 >= r1.y2 && r2.y1 <= r1.y2 && r2.y1 >= r1.y1) {
                overlaps = true;
          //      System.out.println("Case5.3:\t"+r1.relationIndex+","+ r2.relationIndex);
            } else if (r2.y1 >= r1.y1 && r2.y2 <= r1.y2) {
                overlaps = true;
            //    System.out.println("Case5.4:\t"+r1.relationIndex+","+ r2.relationIndex);
            }
        }
    return overlaps;
    }
}

