package GEMS2Latest1;

import Common.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
      //     System.out.println(cellNumber+"\treducerrrrr\t"+ t);
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
    public void joinABCD(double cellWidth, int CellNumber, int cellRow, int cellCol, int cellsPerRow, int cellsPerCol, ArrayList<Rectangle> la, ArrayList<Rectangle> lb, ArrayList<Rectangle> lc, ArrayList<Rectangle> ld, Rectangle cellRect, Context context) throws java.io.IOException, InterruptedException
    {
        ArrayList<Pair> baList = new ArrayList<>();
        ArrayList<Pair> cdList = new ArrayList<>();
        
        boolean[] bCrossCellBound = new boolean[lb.size()];
        boolean[] cCrossCellBound = new boolean[lc.size()];
        
        int joinType1 = 1; int joinType2 = 2; int joinType3 = 3;    int key1;
        
        HashMap<Integer,Integer> bIndMap = new HashMap<>();
        HashMap<Integer,Integer> cIndMap = new HashMap<>();
       
        key1= (cellRow * cellsPerRow) + cellCol;
        ArrayList<HashSet<Integer>> aCellRows = new ArrayList<>(Collections.nCopies(la.size(),null));
        ArrayList<HashSet<Integer>> dCellColumns = new ArrayList<>(Collections.nCopies(ld.size(),null));
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startIndex = baList.size();
            for(int ai = 0;ai<la.size();ai++){
                Rectangle a = la.get(ai);
                if(Common.Rectangle.checkOverlap(b, a)){
                    if(Common.Rectangle.crossCellBoundary(b, cellRect))
                        bCrossCellBound[bi] = true;
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
                    if(Common.Rectangle.crossCellBoundary(c, cellRect))
                        cCrossCellBound[ci] = true;
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
        boolean[] bSent = new boolean[lb.size()];
        boolean[] cSent = new boolean[lc.size()];
        //boolean[] bcSent = new boolean[lb.size()];
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startBIndex = bIndMap.get(bi);
            for(int ci = 0;ci<lc.size();ci++){
                Rectangle c = lc.get(ci);
                if(Common.Rectangle.checkOverlap(b, c)){
                    int startCIndex = cIndMap.get(ci);
                    if(!Common.Rectangle.crossCellBoundary(b,cellRect)&& !Common.Rectangle.crossCellBoundary(c,cellRect)){
                        if(startBIndex<0 || startCIndex<0)  
                            continue;
                        // Generate output tuples iterating over the corresponding baList and cdList
                        for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                            for(int cInd = startCIndex; cInd<cdList.size() && ci == cdList.get(cInd).r1RowNum;cInd++){
                                Pair p1=baList.get(bInd);
                                Pair p2 =cdList.get(cInd);
                                String r11 =  la.get(p1.r2RowNum).rowNum +","+ b.rowNum +","+ c.rowNum +","+ ld.get(p2.r2RowNum).rowNum;
                                mos.write(new LongWritable(key1), new Text(r11), check0);
                            }
                        }
                    }else{
                        // Case 1: both b and c overlap with cell boundary
                        if(Common.Rectangle.crossCellBoundary(b,cellRect) && Common.Rectangle.crossCellBoundary(c,cellRect)){
                            if(startBIndex>=0 && !bSent[bi]){
                                for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                                    // write all ab matching tuples into context
                                    Rectangle a = la.get(baList.get(bInd).r2RowNum);
                                    // Initialize aCellRows entry for the current a at baList.get(bInd).r2RowNum
                                    if(aCellRows.get(baList.get(bInd).r2RowNum)==null){
                                        aCellRows.set(baList.get(bInd).r2RowNum, new HashSet<Integer>());
                                    }
                                    aCellRows.get(baList.get(bInd).r2RowNum).add(b.rowNum%cellsPerRow);
                                    //String r11 =  ","+joinType1 +","+ a.rowNum +","+ a.relationIndex +","+ a.x1 +","+ a.y1 +","+ a.x2 +","+ a.y2 +","+b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2;
                                    //context.write(new LongWritable(key1), new Text(r11));
                                }
                                bSent[bi] = true;
                            }
                            if(startCIndex>=0 && !cSent[ci]){
                                for(int cInd = startCIndex; cInd<cdList.size() && ci==cdList.get(cInd).r1RowNum;cInd++){
                                    // write all cd matching tuples into context
                                    Rectangle d = ld.get(cdList.get(cInd).r2RowNum);
                                    if(dCellColumns.get(cdList.get(cInd).r2RowNum)==null){
                                        dCellColumns.set(cdList.get(cInd).r2RowNum, new HashSet<Integer>());
                                    }
                                    dCellColumns.get(cdList.get(cInd).r2RowNum).add(c.rowNum%cellsPerCol);
                                    //String r11 =  ","+joinType3 +","+ c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2 +","+d.rowNum +","+ d.relationIndex +","+ d.x1 +","+ d.y1 +","+ d.x2 +","+ d.y2;
                                    //context.write(new LongWritable(key1), new Text(r11));
                                }
                                cSent[ci] = true;
                            }
                            // Send bc tuple to the next map-reduce
                            String r11 =  ","+joinType2 +","+ b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2 +","+c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2;
                            context.write(new LongWritable(key1), new Text(r11));
                        }
                        // Case 2: Only b overlaps with cell boundary
                        else if(Common.Rectangle.crossCellBoundary(b,cellRect)){
                            if(startBIndex>=0 && !bSent[bi]){
                                for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                                    // write all ab matching tuples into context
                                    Rectangle a = la.get(baList.get(bInd).r2RowNum);
                                    if(aCellRows.get(baList.get(bInd).r2RowNum)==null){
                                        aCellRows.set(baList.get(bInd).r2RowNum, new HashSet<Integer>());
                                    }
                                    aCellRows.get(baList.get(bInd).r2RowNum).add(b.rowNum%cellsPerRow);
                                    //String r11 =  ","+joinType1 +","+ a.rowNum +","+ a.relationIndex +","+ a.x1 +","+ a.y1 +","+ a.x2 +","+ a.y2 +","+b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2;
                                    //context.write(new LongWritable(key1), new Text(r11));
                                }
                                bSent[bi] = true;
                            }
                            // if there is no cd tuple overlapping with bc 
                            // no need to send bc
                            if(startCIndex<0){
                            }else{ // send all cd pairs along with the bc
                                if(!cSent[ci])
                                    for(int cInd = startCIndex; cInd<cdList.size() && ci==cdList.get(cInd).r1RowNum;cInd++){
                                        // write all cd matching tuples into context
                                        Rectangle d = ld.get(cdList.get(cInd).r2RowNum);
                                        if(dCellColumns.get(cdList.get(cInd).r2RowNum)==null){
                                            dCellColumns.set(cdList.get(cInd).r2RowNum, new HashSet<Integer>());
                                        }
                                        dCellColumns.get(cdList.get(cInd).r2RowNum).add(c.rowNum%cellsPerCol);
                                        //String r11 =  ","+joinType3 +","+ c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2 +","+d.rowNum +","+ d.relationIndex +","+ d.x1 +","+ d.y1 +","+ d.x2 +","+ d.y2;
                                        //context.write(new LongWritable(key1), new Text(r11));
                                    }
                                cSent[ci] = true;
                            }
                            String r11 =  ","+joinType2 +","+ b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2 +","+c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2;
                            context.write(new LongWritable(key1), new Text(r11));
                        }
                        // Case 3: Only c overlaps with cell boundary
                        else{
                            if(startCIndex>=0 && !cSent[ci]){
                                for(int cInd = startCIndex; cInd<cdList.size() && ci==cdList.get(cInd).r1RowNum;cInd++){
                                    // write all cd matching tuples into context
                                    Rectangle d = ld.get(cdList.get(cInd).r2RowNum);
                                    if(dCellColumns.get(cdList.get(cInd).r2RowNum)==null){
                                        dCellColumns.set(cdList.get(cInd).r2RowNum, new HashSet<Integer>());
                                    }
                                    dCellColumns.get(cdList.get(cInd).r2RowNum).add(c.rowNum%cellsPerCol);
                                    //String r11 =  ","+joinType3 +","+ c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2 +","+d.rowNum +","+ d.relationIndex +","+ d.x1 +","+ d.y1 +","+ d.x2 +","+ d.y2;
                                    //context.write(new LongWritable(key1), new Text(r11));
                                }
                                cSent[ci] = true;
                            }if(startBIndex>=0 && !bSent[bi]){
                                for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                                    // write all ab matching tuples into context
                                    Rectangle a = la.get(baList.get(bInd).r2RowNum);
                                    if(aCellRows.get(baList.get(bInd).r2RowNum)==null){
                                        aCellRows.set(baList.get(bInd).r2RowNum, new HashSet<Integer>());
                                    }
                                    aCellRows.get(baList.get(bInd).r2RowNum).add(b.rowNum%cellsPerRow);
                                    //String r11 =  ","+joinType1 +","+ a.rowNum +","+ a.relationIndex +","+ a.x1 +","+ a.y1 +","+ a.x2 +","+ a.y2 +","+b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2;
                                    //context.write(new LongWritable(key1), new Text(r11));
                                }
                                bSent[bi] = true;
                            }
                            String r11 =  ","+joinType2 +","+ b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2 +","+c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2;
                            context.write(new LongWritable(key1), new Text(r11));
                        }
                    }
                }
            }
        }
        for(int bi = 0;bi<lb.size();bi++){
            if(!bSent[bi] && bCrossCellBound[bi]){
                for(int bii = bIndMap.get(bi);bii<baList.size();bii++){
                    Pair p = baList.get(bii);
                    if(p.r1RowNum!=bi){
                        break;
                    }
                    Rectangle a = la.get(baList.get(bii).r2RowNum);
                    Rectangle b = lb.get(baList.get(bii).r1RowNum);  
                    if(aCellRows.get(baList.get(bii).r2RowNum)==null){
                        aCellRows.set(baList.get(bii).r2RowNum, new HashSet<Integer>());
                    }
                    aCellRows.get(baList.get(bii).r2RowNum).add(b.rowNum%cellsPerRow);
                    //String r11 =  ","+joinType1 +","+ a.rowNum +","+ a.relationIndex +","+ a.x1 +","+ a.y1 +","+ a.x2 +","+ a.y2 +","+b.rowNum +","+ b.relationIndex +","+ b.x1 +","+ b.y1 +","+ b.x2 +","+ b.y2;
                    //context.write(new LongWritable(key1), new Text(r11));
                }
            }bSent[bi] = true;
        }
        for(int ci = 0;ci<lc.size();ci++){
            if(!cSent[ci] && cCrossCellBound[ci]){
                for(int cii = cIndMap.get(ci);cii<cdList.size();cii++){
                    Pair p = cdList.get(cii);
                    if(p.r1RowNum!=ci){
                        break;
                    }
                    Rectangle c = lc.get(cdList.get(cii).r1RowNum);
                    Rectangle d = ld.get(cdList.get(cii).r2RowNum);
                    if(dCellColumns.get(cdList.get(cii).r2RowNum)==null){
                        dCellColumns.set(cdList.get(cii).r2RowNum, new HashSet<Integer>());
                    }
                    dCellColumns.get(cdList.get(cii).r2RowNum).add(c.rowNum%cellsPerCol);
                    //String r11 =  ","+joinType3 +","+ c.rowNum +","+ c.relationIndex +","+ c.x1 +","+ c.y1 +","+ c.x2 +","+ c.y2 +","+d.rowNum +","+ d.relationIndex +","+ d.x1 +","+ d.y1 +","+ d.x2 +","+ d.y2;
                    //context.write(new LongWritable(key1), new Text(r11));
                }
            }cSent[ci] = true;
        }
        for(int ai = 0;ai<la.size();ai++){
            if(aCellRows.get(ai)!=null){
                Rectangle a = la.get(ai);
                String r11 = ","+ joinType1 +","+ a.rowNum +","+ a.relationIndex +","+ a.x1 +","+ a.y1 +","+ a.x2 +","+ a.y2 ;
                
                for(Integer cr:aCellRows.get(ai)){
                    r11 +="," +cr ;
                }
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
        for(int di =0;di<ld.size();di++){
            if(dCellColumns.get(di)!=null){
                Rectangle d =ld.get(di);
                String r11 = ","+ joinType3 +","+ d.rowNum +","+ d.relationIndex +","+d.x1+","+d.y1+","+d.x2+","+d.y2;
                for(Integer cc :dCellColumns.get(di)){
                    r11 += ","+cc;
                }
                context.write(new LongWritable(key1), new Text(r11));
            }
        }
    }  
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        startr1 = System.currentTimeMillis() - startr1;
        System.out.println("-------------------------------------------");
        System.out.println("GEMS R1 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1);
        System.out.println("-------------------------------------------");
    }
}