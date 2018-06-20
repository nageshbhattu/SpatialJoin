package abcSequential;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, LongWritable, ABJoinTuple>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();   
        int max = conf.getInt("gridMax",0);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;
        
        int JoinType, rowNum1, relationIndex1, rowNum2, relationIndex2;
        double x11, y11, x12, y12, x21, y21, x22, y22;
        
        String words[] = value.toString().split(",");
        
        relationIndex1 = Integer.parseInt(words[1]);
        if(relationIndex1 == 1){    //ab join tuple will be processed here{1 means relation index of A}
            rowNum1 = Integer.parseInt(words[0]);
            x11 = Double.parseDouble(words[2]);
            y11 = Double.parseDouble(words[3]);
            x12 = Double.parseDouble(words[4]);
            y12 = Double.parseDouble(words[5]);
            rowNum2 = Integer.parseInt(words[6]);
            relationIndex2 = Integer.parseInt(words[7]);
            x21 = Double.parseDouble(words[8]);
            y21 = Double.parseDouble(words[9]);
            x22 = Double.parseDouble(words[10]);
            y22 = Double.parseDouble(words[11]);
            JoinType =Integer.parseInt(words[12]);
            
            ABJoinTuple jointuple = new ABJoinTuple(rowNum1, relationIndex1, x11, y11, x12, y12, rowNum2, relationIndex2, x21, y21, x22, y22, JoinType);		 
            double[] x = {x21,x22};
            double[] y = {y21,y22};

            int x1 = (int) Math.floor(x[0] / cellWidth);
            int x2 = (int) Math.floor(x[1] / cellWidth);
            int y1 = (int) Math.floor(y[0] / cellHeight);
            int y2 = (int) Math.floor(y[1] / cellHeight);

            for (int j = y1; j <= y2; j++) {
                for (int i = x1; i <= x2; i++) {
                    int mapkey = ((j * numOfReducersPerRow) + i);
                    context.write(new LongWritable(mapkey), jointuple);
                }
            }
        }
        else{   //C will be processed here(relation index 3)
            rowNum1 = Integer.parseInt(words[0]);
            x11 = Double.parseDouble(words[2]);
            y11 = Double.parseDouble(words[3]);
            x12 = Double.parseDouble(words[4]);
            y12 = Double.parseDouble(words[5]);
            rowNum2 = 0 ;
            relationIndex2 = 0;
            x21 = 0.0; 
            y21 = 0.0; 
            x22 = 0.0; 
            y22 = 0.0;
            JoinType=0;
            ABJoinTuple jointuple = new ABJoinTuple(rowNum1, relationIndex1, x11, y11, x12, y12, rowNum2, relationIndex2, x21, y21, x22, y22, JoinType);		 
        
            double[] x = {x11,x12};
            double[] y = {y11,y12};

            int x1 = (int) Math.floor(x[0] / cellWidth);
            int x2 = (int) Math.floor(x[1] / cellWidth);
            int y1 = (int) Math.floor(y[0] / cellHeight);
            int y2 = (int) Math.floor(y[1] / cellHeight);

            for (int j = y1; j <= y2; j++) {
                for (int i = x1; i <= x2; i++) {
                    int mapkey = ((j * numOfReducersPerRow) + i);
                    context.write(new LongWritable(mapkey), jointuple);
                }
            }
        } 
    }
}