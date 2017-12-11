package Sequential;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperSeqForD extends Mapper<LongWritable, Text, LongWritable, ABCJoinTuple> 
{
   @Override
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
   {
        String[] line = value.toString().trim().split(",");
        int JoinType=0;
        int rowNum1 = Integer.parseInt(line[0]);
	int relationIndex1 = Integer.parseInt(line[1]);
	double x11 = Double.parseDouble(line[2]);
	double y11 = Double.parseDouble(line[3]);
	double x12 = Double.parseDouble(line[4]);
	double y12 = Double.parseDouble(line[5]);
	//Rectangle R1 = new Rectangle(rowNum1, relationIndex1,x11,y11,x12,y12);
	int rowNum2 = 0 ;
	int relationIndex2 = 0;
	double x21 = 0.0; 
	double y21 = 0.0; 
	double x22 = 0.0;
	double y22 = 0.0;
                         
        int rowNum3 = 0;
	int relationIndex3 = 0;
	double x31 = 0.0;
	double y31 = 0.0;
	double x32 = 0.0;
	double y32 = 0.0;
        int abcType=2;
	
        ABCJoinTuple jointuple = new ABCJoinTuple(abcType,JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22,rowNum3,relationIndex3,x31,y31,x32,y32);
        
        Configuration conf = context.getConfiguration();   
        int max = conf.getInt("gridMax",0);
    //      System.out.println("max is--:"+max);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
      //   System.out.println("numOfReducers:--"+numOfReducersPerRow);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;
        
        double[] x = {x11,x12};
        double[] y = {y11,y12};
        int x1 = (int) Math.floor(x[0] / cellWidth);
        int x2 = (int) Math.floor(x[1] / cellWidth);
        int y1 = (int) Math.floor(y[0] / cellHeight);
        int y2 = (int) Math.floor(y[1] / cellHeight);

        for (int j = y1; j <= y2; j++) 
        {
            for (int i = x1; i <= x2; i++) 
            {
                int mapkey = ((j * numOfReducersPerRow) + i);
            //    System.out.println("MapperD--> " + mapkey + " " + jointuple);
                context.write(new LongWritable(mapkey), jointuple);
            }
        }
    }
}