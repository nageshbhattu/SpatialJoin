package GEMS2Latest1;
import Common.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce2 extends Reducer<LongWritable, Rectangle, LongWritable, Text> 
{
    long startr2;
    @Override
    protected void setup(Context context) throws IOException{
        startr2 = System.currentTimeMillis();
    }
    @Override
    public void reduce(LongWritable key, Iterable<Rectangle> value,Context context) throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();

        int cellWidth = (int)conf.getDouble("cellWidth", 0.0D);
        int bCellRow, cCellRow;// = cellNumber / cellsPerRow;
        int bCellCol, cCellCol;// = cellNumber % cellsPerRow;
        
        
        TreeSet<Rectangle> laa = new TreeSet<>();
        TreeSet<Rectangle> lbb = new TreeSet<>();
        TreeSet<Rectangle> lcc = new TreeSet<>();   
        TreeSet<Rectangle> ldd = new TreeSet<>();   
        int key1 = (int) key.get();
        
        for(Rectangle jt:value)
        {
            
            Rectangle t = new Rectangle(jt);
            switch (t.relationIndex) {
                case 1:
                    laa.add(t);
                    break;
                case 2:
                    lbb.add(t);
                    break;
                case 3:
                    lcc.add(t);
                    break;
                case 4:
                    ldd.add(t);
                    break;
                default:
                    break;
            }
        }
        ArrayList<Rectangle> la = new ArrayList<>(laa);
        ArrayList<Rectangle> lb = new ArrayList<>(lbb);
        ArrayList<Rectangle> lc = new ArrayList<>(lcc);
        ArrayList<Rectangle> ld = new ArrayList<>(ldd);
        
        ArrayList<Pair> baList = new ArrayList<>();
        ArrayList<Pair> cdList = new ArrayList<>();
        
        HashMap<Integer,Integer> bIndMap = new HashMap<>();
        HashMap<Integer,Integer> cIndMap = new HashMap<>();
        for(int bi= 0;bi<lb.size();bi++){
            Rectangle b = lb.get(bi);
            int startIndex = baList.size();
            for(int ai = 0;ai<la.size();ai++){
                Rectangle a = la.get(ai);
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
        
        for(int ci= 0;ci<lc.size();ci++){
            Rectangle c = lc.get(ci);
            int startIndex = cdList.size();
            for(int di = 0;di<ld.size();di++){
                Rectangle d = ld.get(di);
                if(Common.Rectangle.checkOverlap(c, d)){
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
     
        for(int bi= 0; bi<lb.size(); bi++){
            Rectangle b = lb.get(bi);
            int startBIndex = bIndMap.get(bi);
            bCellRow = (int)Math.floor(b.y1/cellWidth);
            bCellCol = (int)Math.floor(b.x1/cellWidth); //here cellWidth should cellHeight but in our case width == height
            Rectangle bCellRect = new Rectangle(0, 0, bCellCol * cellWidth, bCellRow * cellWidth, (bCellCol + 1) * cellWidth, (bCellRow + 1) * cellWidth);
            for(int ci = 0;ci<lc.size();ci++){
                Rectangle c = lc.get(ci);
                int startCIndex = cIndMap.get(ci);
                cCellRow = (int)Math.floor(c.y1/cellWidth);
                cCellCol = (int)Math.floor(c.x1/cellWidth); //here cellWidth should cellHeight but in our case width == height
                Rectangle cCellRect = new Rectangle(0, 0, cCellCol * cellWidth, cCellRow * cellWidth, (cCellCol + 1) * cellWidth, (cCellRow + 1) * cellWidth);
                //   context.getCounter(MyCounters.BC_OVERLAP_COUNT).increment(1);
                if(Common.Rectangle.checkOverlap(b, c) && (Common.Rectangle.crossCellBoundary(b, bCellRect) || Common.Rectangle.crossCellBoundary(c, cCellRect))){
                    if(startBIndex>=0 && startCIndex >=0){
                        for(int bInd = startBIndex; bInd<baList.size() && bi==baList.get(bInd).r1RowNum;bInd++){
                            Rectangle a = la.get(baList.get(bInd).r2RowNum);
                            for(int cInd = startCIndex; cInd<cdList.size() && ci==cdList.get(cInd).r1RowNum;cInd++){
                                Rectangle d = ld.get(cdList.get(cInd).r2RowNum);
                                String output = a.rowNum +","+ b.rowNum +","+ c.rowNum +","+ d.rowNum ;
                                context.write(new LongWritable(key1), new Text(output));
                            }
                        }
                    }
                }
            }   
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException{
        startr2 = System.currentTimeMillis() - startr2;
        System.out.println("-------------------------------------------");
        System.out.println("GEMS R2 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr2);
        System.out.println("-------------------------------------------");
    }
}