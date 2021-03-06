package abcCREPnew;
import Common.*;
import abcCREPnew.CREPDriver.MyCounters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce2 extends Reducer<LongWritable,Rectangle,LongWritable,Text> 
{
    long startr2;
    @Override
    protected void setup(Context context) throws IOException{
        startr2 = System.currentTimeMillis();
    }
    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> value,Context context) throws IOException, InterruptedException 
    {
        TreeSet<Rectangle> laa = new TreeSet<>();
        TreeSet<Rectangle> lbb = new TreeSet<>();
        TreeSet<Rectangle> lcc = new TreeSet<>();
        
        Configuration conf = context.getConfiguration();
        
        double cellWidth = conf.getDouble("cellWidth", 0.0);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);
        int cellNumber =(int) key.get();
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        Rectangle cellRect = new Rectangle(0,0,cellCol * cellWidth,cellRow * cellWidth,
                                                (cellCol+1) * cellWidth,(cellRow +1) * cellWidth);
        for(Rectangle t:value)
        {
            Rectangle newt = new Rectangle(t);
            switch (t.relationIndex) {
                case 1:
                    laa.add(newt);
                    break;
                case 2:
                    lbb.add(newt);
                    break;
                case 3:
                    lcc.add(newt);
                    break;
                default:
                    break;
            }
        }
        ArrayList<Rectangle> la = new ArrayList<>(laa);
        ArrayList<Rectangle> lb = new ArrayList<>(lbb);
        ArrayList<Rectangle> lc = new ArrayList<>(lcc);
        
        join(cellNumber, cellRow, cellCol, cellsPerRow, la, lb, lc, cellRect,context);
    }
    public void join(int CellNumber, int cellRow, int cellCol, int cellsPerRow, ArrayList<Rectangle> la, ArrayList<Rectangle> lb, ArrayList<Rectangle> lc, Rectangle cellRect,
                                    Context context) throws IOException, InterruptedException 
    {
        
        int key1;
        key1= (cellRow * cellsPerRow) + cellCol;
        
        ArrayList<Pair> baList = new ArrayList<>();
        HashMap<Integer,Integer> bIndMap = new HashMap<>();
        
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startIndex = baList.size();
            for(int ai = 0;ai<la.size();ai++){
                Rectangle a = la.get(ai);
            //    context.getCounter(MyCounters.BA_OVERLAP_COUNT).increment(1);
                if(Common.Rectangle.checkOverlap(b, a)){
                    Pair p = new Pair(bi,ai);
                    baList.add(p);
                }
            }
            int endIndex = baList.size();
            if(endIndex>startIndex){
                bIndMap.put(bi, startIndex);
            }else{
                bIndMap.put(bi, -1);
            }
        }
        for(int bi= 0; bi<lb.size(); bi++){
            Rectangle b = lb.get(bi);
            int startBIndex = bIndMap.get(bi);
            for(int ci = 0;ci<lc.size();ci++){
                Rectangle c = lc.get(ci);
             //   context.getCounter(MyCounters.BC_OVERLAP_COUNT).increment(1);
                if(Common.Rectangle.checkOverlap(b, c) && (Common.Rectangle.crossCellBoundary(b, cellRect) || Common.Rectangle.crossCellBoundary(c, cellRect))){
                    if(startBIndex>=0 ){
                        for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                            Rectangle a = la.get(baList.get(bInd).r2RowNum);
                            double xx=checkRightMostRectangle(a,b,c);
                            double yy=checkLowerMostRectangle(a,b,c);
                            String output = a.rowNum +","+ b.rowNum +","+ c.rowNum ;
                            if((cellRect.x1 <= xx) && (cellRect.x2 > xx) && (cellRect.y1 <= yy) && (cellRect.y2 > yy))
                            {
                                context.write(new LongWritable(key1), new Text(output));
                            }
                        }
                    }
                }
            }   
        }
    }
    double checkRightMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3) 
    {
        double x=0;
        double p=(r1.x1 > r2.x1)?r1.x1:r2.x1;
        double q=(p > r3.x1)?p:r3.x1;
        if(q==r1.x1) {
            x=r1.x1;
        }
        else if(q==r2.x1) {
            x=r2.x1;
        }
        else if(q==r3.x1) {
            x=r3.x1;
        }
        return x;   // It will Return Right Most rectangle's  x1 point
    }
    double checkLowerMostRectangle(Rectangle r1, Rectangle r2, Rectangle r3) 
    {
        double y=0;
        double p=(r1.y1 > r2.y1)?r1.y1:r2.y1;
        double q=(p > r3.y1)?p:r3.y1;
        if(q==r1.y1) {
            y=r1.y1;
        }
        else if(q==r2.y1) {
            y=r2.y1;
        }
        else if(q==r3.y1) {
            y=r3.y1;
        }
        return y;   // It will Return Upper Most rectangle's  y1 point
    }
 
    @Override
    protected void cleanup(Context context) throws IOException{
        startr2 = System.currentTimeMillis() - startr2;
        System.out.println("-------------------------------------------");
        System.out.println("3rCRep R2 IDr2 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr2);
        System.out.println("-------------------------------------------");
    }
}