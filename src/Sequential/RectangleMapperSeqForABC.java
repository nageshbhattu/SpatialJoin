package Sequential;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperSeqForABC extends Mapper<LongWritable, Text,LongWritable , ABCJoinTuple>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
       // System.out.println("Mapper ABC \t"+ value.toString());
        String words[] = value.toString().split(",");
	String[] subWords = words[0].split("\\s+");
			   
	int JoinType=0;
	JoinType =Integer.parseInt(subWords[subWords.length-1]);
	
        int rowNum1 = Integer.parseInt(words[2]);
	int relationIndex1 = Integer.parseInt(words[3]);
	double x11 = Double.parseDouble(words[4]);
	double y11 = Double.parseDouble(words[5]);
	double x12 = Double.parseDouble(words[6]);
	double y12 = Double.parseDouble(words[7]);
	//Rectangle R1 = new Rectangle(rowNum1, relationIndex1,x11,y11,x12,y12);
	int rowNum2 = Integer.parseInt(words[8]);
	int relationIndex2 = Integer.parseInt(words[9]);
	double x21 = Double.parseDouble(words[10]);
	double y21 = Double.parseDouble(words[11]);
	double x22 = Double.parseDouble(words[12]);
	double y22 = Double.parseDouble(words[13]);
			   
        int rowNum3 = Integer.parseInt(words[15]);
	int relationIndex3 = Integer.parseInt(words[16]);
	double x31 = Double.parseDouble(words[17]);
	double y31 = Double.parseDouble(words[18]);
	double x32 = Double.parseDouble(words[19]);
	double y32 = Double.parseDouble(words[20]);
        int abcType=1;
        ABCJoinTuple jointuple = new ABCJoinTuple(abcType,JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22,rowNum3,relationIndex3,x31,y31,x32,y32);		 
        Configuration conf = context.getConfiguration();   
        int max = conf.getInt("gridMax",0);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;
        double[] x = {x31,x32};
        double[] y = {y31,y32};
      //  int relation = 0;
     
      //  RectangleSeq r = new RectangleSeq(rowNum2, relationIndex2, x[0], y[0], x[1], y[1]);

            int x1 = (int) Math.floor(x[0] / cellWidth);
            int x2 = (int) Math.floor(x[1] / cellWidth);
            int y1 = (int) Math.floor(y[0] / cellHeight);
            int y2 = (int) Math.floor(y[1] / cellHeight);

                for (int j = y1; j <= y2; j++) {
                    for (int i = x1; i <= x2; i++) {
                        int mapkey = ((j * numOfReducersPerRow) + i);
          //              System.out.println("MapperABC----> " + mapkey+ " \t" +jointuple);
                        context.write(new LongWritable(mapkey), jointuple);
                    }
                }
    }
}                 