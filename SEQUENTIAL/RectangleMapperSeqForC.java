package Sequential;

import Sequential.SpatialJoinDriverSeq.MyCounters;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperSeqForC extends Mapper<LongWritable, Text, LongWritable, ABJoinTuple> 
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int abType=2;
        String[] line = value.toString().trim().split(",");
        int JoinType=0;
        int r1RowNum = Integer.parseInt(line[0]);
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
	ABJoinTuple jointuple = new ABJoinTuple(abType,JoinType,r1RowNum,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22);		 
        
        Configuration conf = context.getConfiguration();   
        int max = conf.getInt("gridMax",0);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;
        
        double[] x = {x11,x12};
        double[] y = {y11,y12};
 
        int x1 = (int) Math.floor(x[0] / cellWidth);
        int x2 = (int) Math.floor(x[1] / cellWidth);
        int y1 = (int) Math.floor(y[0] / cellHeight);
        int y2 = (int) Math.floor(y[1] / cellHeight);

        for (int j = y1; j <= y2; j++) {
            for (int i = x1; i <= x2; i++) {
                int mapkey = ((j * numOfReducersPerRow) + i);
          //      System.out.println("MapperC----->" + mapkey  + " \t " + jointuple);
                context.write(new LongWritable(mapkey), jointuple);
            //    context.getCounter(MyCounters.C_COUNT).increment(1);
            }
        }
    }
}