package abcdCREPv2;
import Common.*;

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
        ArrayList<Rectangle> ld = new ArrayList();

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
        int key1;
        key1= (cellRow * cellsPerRow) + cellCol;
        
        ArrayList<Pair> baList = new ArrayList<>();
        ArrayList<Pair> cdList = new ArrayList<>();
        
        boolean[] pla = new boolean[la.size()];
        boolean[] plb = new boolean[lb.size()];
        boolean[] plc = new boolean[lc.size()];
        boolean[] pld = new boolean[ld.size()];
        
        boolean[] aCrossCellBound = new boolean[la.size()];
        boolean[] bCrossCellBound = new boolean[lb.size()];
        boolean[] cCrossCellBound = new boolean[lc.size()];
        boolean[] dCrossCellBound = new boolean[ld.size()];
        
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
        for(int di=0;di<ld.size();di++){
            if(Common.Rectangle.project(ld.get(di),cellRect))
                pld[di] = true;
            if(Common.Rectangle.crossCellBoundary(ld.get(di), cellRect))
                dCrossCellBound[di] = true;
        }
        
        HashMap<Integer,Integer> bIndMap = new HashMap<>();
        HashMap<Integer,Integer> cIndMap = new HashMap<>();
        
        boolean[] aSent = new boolean[la.size()];
        boolean[] bSent = new boolean[lb.size()];
        boolean[] cSent = new boolean[lc.size()];
        boolean[] dSent = new boolean[ld.size()];
        
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startIndex = baList.size();
            for(int ai = 0;ai<la.size();ai++){
                Rectangle a = la.get(ai);
                if(Common.Rectangle.checkOverlap(b, a)){
                    if(bCrossCellBound[bi]){
                        bSent[bi] = true;
                        aSent[ai] = true;
                    }
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
        
        for(int ci= 0;ci<lc.size();ci++){
            Rectangle c = lc.get(ci);
            int startIndex = cdList.size();
            for(int di = 0;di<ld.size();di++){
                Rectangle d = ld.get(di);
                if(Common.Rectangle.checkOverlap(c, d)){
                    if(cCrossCellBound[ci]){
                        cSent[ci] = true;
                        dSent[di] = true;
                    }
                    Pair p = new Pair(ci, di);
                    cdList.add(p);
                }
            }
            int endIndex = cdList.size();
            if(endIndex>startIndex){
                cIndMap.put(ci, startIndex);
            }else{
                cIndMap.put(ci, -1);
            }
        }
        
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startBIndex = bIndMap.get(bi);
            for(int ci = 0;ci<lc.size();ci++){
                Rectangle c = lc.get(ci);
                if(Common.Rectangle.checkOverlap(b, c)){
                    int startCIndex = cIndMap.get(ci);
                    if(!bCrossCellBound[bi] && !cCrossCellBound[ci]){
                        if(startBIndex<0 || startCIndex<0)
                            continue;
                        // Generate output tuples iterating over the corresponding baList and cdList
                        for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                            for(int cInd = startCIndex; cInd<cdList.size() && ci == cdList.get(cInd).r1RowNum;cInd++){
                                Rectangle a = la.get(baList.get(bInd).r2RowNum);
                                Rectangle d = ld.get(cdList.get(cInd).r2RowNum);
                                String r11 =  a.rowNum +","+ b.rowNum +","+ c.rowNum +","+ d.rowNum;
                                mos.write(new LongWritable(key1), new Text(r11), check0);
                            }
                        }
                    }else{
                        // Case 1: both b and c overlap with cell boundary
                        // Case 2: Only b overlaps with cell boundary
                        if(Common.Rectangle.crossCellBoundary(b,cellRect)){
                            // if there is no cd tuple overlapping with bc 
                            // no need to send bc
                            if(startCIndex<0){
                            }else{ // send all cd pairs along with the bc
                                if(!cSent[ci])
                                    for(int cInd = startCIndex; cInd<cdList.size() && ci==cdList.get(cInd).r1RowNum;cInd++){
                                        // write all cd matching tuples into context
                                        dSent[cdList.get(cInd).r2RowNum] = true;
                                    }
                            }
                            cSent[ci] = true;
                        }
                        // Case 3: Only c overlaps with cell boundary
                        else{
                            if(startBIndex < 0 ){
                            }else{
                                if(!bSent[bi])
                                    for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                                        // write all ab matching tuples into context
                                        aSent[baList.get(bInd).r2RowNum] = true;
                                    }
                            }
                            bSent[bi] = true;
                        }
                    }
                }
            }
        }
    
        for(int i = 0;i<la.size();i++){
           if(pla[i] && (aSent[i] || aCrossCellBound[i]))
           {
                Rectangle r1 = la.get(i);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(ra[i]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
        for(int i = 0;i<lb.size();i++){
            if(plb[i] && (bSent[i] || bCrossCellBound[i])){
                Rectangle r1 = lb.get(i);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(rb[i]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
        for(int i = 0;i<lc.size();i++){
            if(plc[i] && (cSent[i] || cCrossCellBound[i])){
                Rectangle r1 = lc.get(i);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(rc[i]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
        for(int i = 0;i<ld.size();i++){
            if(pld[i] && (dSent[i] || dCrossCellBound[i])){
                Rectangle r1 = ld.get(i);
                String r11 =  ","+r1.rowNum +","+ r1.relationIndex +","+ r1.x1 +","+ r1.y1 +","+ r1.x2 +","+ r1.y2 +","+ 1;//(rd[i]?1:0);
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        startr1 = System.currentTimeMillis() - startr1;
        mos.close();
        System.out.println("-------------------------------------------");
        System.out.println("4RCRep R1 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1);
        System.out.println("-------------------------------------------");
    }
}