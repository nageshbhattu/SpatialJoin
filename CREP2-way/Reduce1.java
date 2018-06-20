package abcCREPnew;
import Common.*;
//import abcCREPnew.CREPDriver.MyCounters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
public class Reduce1 extends Reducer<LongWritable, Rectangle, LongWritable, Text>
{
    long startr1;
    MultipleOutputs<LongWritable,Text> mos;
    @Override
    public void setup(Context context){
        mos = new MultipleOutputs(context);
        startr1 = System.currentTimeMillis();
    }
    static String check0 = "check0";
    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> values, Context context) throws IOException, InterruptedException
    {
        ArrayList<Rectangle> la = new ArrayList();
        ArrayList<Rectangle> lb = new ArrayList();
        ArrayList<Rectangle> lc = new ArrayList();
       
        Configuration conf = context.getConfiguration();

        double cellWidth = conf.getDouble("cellWidth", 0.0D);
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        int cellsPerCol = conf.getInt("p1NumOfReducersPerCol", 0);
        
        int cellNumber = (int)key.get();
        int cellRow = cellNumber / cellsPerRow;
        int cellCol = cellNumber % cellsPerRow;
        Rectangle cellRect = new Rectangle(0, 0, cellCol * cellWidth, cellRow * cellWidth, (cellCol + 1) * cellWidth, (cellRow + 1) * cellWidth);
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
                default:
                    break;
            }
        }
        joinABCD(cellWidth, cellNumber, cellRow, cellCol, cellsPerRow, cellsPerCol, la, lb, lc, cellRect, context);
    }
    
    public void joinABCD(double cellWidth, int CellNumber, int cellRow, int cellCol, int cellsPerRow, int cellsPerCol, ArrayList<Rectangle> la, ArrayList<Rectangle> lb, ArrayList<Rectangle> lc, Rectangle cellRect, Reducer<LongWritable, Rectangle, LongWritable, Text>.Context context) throws java.io.IOException, InterruptedException
    {
        boolean[] ra = new boolean[la.size()];
        boolean[] rb = new boolean[lb.size()];
        boolean[] rc = new boolean[lc.size()];
        
        boolean[] pla = new boolean[la.size()];
        boolean[] plb = new boolean[lb.size()];
        boolean[] plc = new boolean[lc.size()];
        
        boolean[] aCrossCellBound = new boolean[la.size()];
        boolean[] bCrossCellBound = new boolean[lb.size()];
        boolean[] cCrossCellBound = new boolean[lc.size()];
        
        for(int ai=0;ai<la.size();ai++){
            if(Common.Rectangle.project(la.get(ai),cellRect))
                pla[ai] = true;
            if(Common.Rectangle.crossCellBoundary(la.get(ai), cellRect))
                aCrossCellBound[ai] = true; 
        }
        for(int bi=0;bi<lb.size();bi++){
            if(Common.Rectangle.project(lb.get(bi),cellRect))
                plb[bi] = true;
            if(Common.Rectangle.crossCellBoundary(lb.get(bi), cellRect))
                bCrossCellBound[bi] = true; 
        }
        for(int ci=0;ci<lc.size();ci++){
            if(Common.Rectangle.project(lc.get(ci),cellRect))
                plc[ci] = true;
            if(Common.Rectangle.crossCellBoundary(lc.get(ci), cellRect))
                cCrossCellBound[ci] = true;
        }
        int key1 = (cellRow * cellsPerRow) + cellCol;
        HashMap<Integer,Integer> bIndMap = new HashMap<>();
        ArrayList<Pair> baList = new ArrayList<>();
        
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startIndex = baList.size();
            for(int ai = 0;ai<la.size();ai++){
                Rectangle a = la.get(ai);
             //   context.getCounter(MyCounters.BA_OVERLAP_COUNT).increment(1);
                if(Common.Rectangle.checkOverlap(b, a)){
                    if(bCrossCellBound[bi]){
                            rb[bi] = true;
                            ra[ai] = true;
                    }
                    Pair p = new Pair(bi,ai);
                    baList.add(p);
                }
                int endIndex = baList.size();
                if(endIndex>startIndex){
                    bIndMap.put(bi, startIndex);
                }else{
                    bIndMap.put(bi, -1);
                }
            }
        }
        for(int bi= 0; bi<lb.size(); bi++){
            Rectangle b = lb.get(bi);
            int startBIndex = bIndMap.get(bi);
            for(int ci = 0;ci<lc.size();ci++){
                Rectangle c = lc.get(ci);
            //    context.getCounter(MyCounters.BC_OVERLAP_COUNT).increment(1);
                if(Common.Rectangle.checkOverlap(b, c)){
                    if(bCrossCellBound[bi]){
                        rb[bi] = true;
                        rc[ci] = true;
                    }
                    else{
                        if(startBIndex>=0)
                            for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                                Rectangle a = la.get(baList.get(bInd).r2RowNum);
                                String output = a.rowNum +","+ b.rowNum +","+ c.rowNum;
                                mos.write(new LongWritable(key1), new Text(output), check0);
                            }
                    }
                }
            }   
        }
        
        for (int ai = 0; ai < la.size(); ai++) {
            if (pla[ai] && (ra[ai] || aCrossCellBound[ai])) {
                Rectangle r1 = la.get(ai);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(ra[ai]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
        for (int bi = 0; bi < lb.size(); bi++) {
            if (plb[bi] && (rb[bi] || bCrossCellBound[bi])) {
                Rectangle r1 = lb.get(bi);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(ra[ai]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
        for (int ci = 0; ci < lc.size(); ci++) {
            if (plc[ci] && (rc[ci] || cCrossCellBound[ci])) {
                Rectangle r1 = lc.get(ci);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(rc[ci]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException{
        startr1 = System.currentTimeMillis() - startr1;
        System.out.println("-------------------------------------------");
        System.out.println("3rCRep R1 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1);
        System.out.println("-------------------------------------------");
    }
}