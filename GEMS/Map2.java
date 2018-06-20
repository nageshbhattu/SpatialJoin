package GEMS2Latest1;

import Common.*;
import GEMS2Latest1.GEMSDriver.MyCounters;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text,LongWritable , Rectangle>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        
        Configuration conf = context.getConfiguration();
        int reducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
	int reducersPerCol = conf.getInt("p1NumOfReducersPerRow", 0);
        int key1 = (int) key.get();
        String words[] = value.toString().split(",");
	
        int JoinType =Integer.parseInt(words[1]);
	int rowNum1 = Integer.parseInt(words[2]);
	int relationIndex1 = Integer.parseInt(words[3]);
	double x11 = Double.parseDouble(words[4]);
	double y11 = Double.parseDouble(words[5]);
	double x12 = Double.parseDouble(words[6]);
	double y12 = Double.parseDouble(words[7]);
	
        
   
        Rectangle r1 = new Rectangle(rowNum1,relationIndex1,x11,y11,x12,y12);
        
        //JoinTuple jointuple = new JoinTuple(JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22);
        switch (JoinType) {
            case 1:
                {
                    context.getCounter(MyCounters.ABTYPE).increment(1);
                    for (int cr=0; cr < words.length-8; cr++ ){
                        Integer reducerRow = Integer.parseInt(words[cr+8]);
                        /*************************************************/
                        context.getCounter(MyCounters.TAU_P).increment(words.length-8);
                        /***************************************************/
                        for(int i=0;i<reducersPerRow;i++)
                        {
                            int reducerIndex=(reducerRow*reducersPerRow)+i;
                            context.write(new LongWritable(reducerIndex),r1);
                            context.getCounter(MyCounters.ABREPLICATE).increment(1);
                        }       
                    }
                    break;
                }
                
            case 2:
                {
                    /*************************************************/
                 //       System.out.println("qrrrr"+key1+"\t:"+(words.length-8));
                        
                        /***************************************************/
                //    context.getCounter(MyCounters.BCTYPE).increment(1);
                    int rowNum2 = Integer.parseInt(words[8]);
                    int relationIndex2 = Integer.parseInt(words[9]);
                    double x21 = Double.parseDouble(words[10]);
                    double y21 = Double.parseDouble(words[11]);
                    double x22 = Double.parseDouble(words[12]);
                    double y22 = Double.parseDouble(words[13]);
                    Rectangle r2 = new Rectangle(rowNum2,relationIndex2,x21,y21,x22,y22);
                    int reducerRow=rowNum1%reducersPerCol;
                    int reducerCol=rowNum2%reducersPerRow;
                    int reducerIndex=(reducerRow*reducersPerRow)+reducerCol;
                    context.write(new LongWritable(reducerIndex),r1);
                    context.write(new LongWritable(reducerIndex),r2);
                    break;
                    
                }
            case 3:
                {
                    context.getCounter(MyCounters.CDTYPE).increment(1);
                   //
                     for (int cc=0; cc < words.length-8; cc++ ){
                        Integer reducerCol = Integer.parseInt(words[cc+8]);
                        /*************************************************/
                        context.getCounter(MyCounters.TAU_S).increment(1);
                        /***************************************************/
                        for(int i=0;i<reducersPerCol;i++)
                        {
                            int reducerIndex=(reducersPerRow*i)+reducerCol;
                            context.write(new LongWritable(reducerIndex),r1);
                            context.getCounter(MyCounters.CDREPLICATE).increment(1);
                        }       
                    }
                     break;
            //default:
               // break;
                }
        }
    }
}