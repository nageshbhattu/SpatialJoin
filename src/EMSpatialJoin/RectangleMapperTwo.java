package EMSpatialJoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperTwo extends Mapper<LongWritable, Text,LongWritable , JoinTuple>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        
     //   int TotalNumofReducers = conf.getInt("p1NumOfReducers", 0);
	int reducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
	int reducersPerCol = conf.getInt("p1NumOfReducersPerRow", 0);
        
	//   System.out.println("Map2------->\t" + TotalNumofReducers +","+reducersPerRow +","+reducersPerCol);
	String words[] = value.toString().split(",");
	String[] subWords = words[0].split("\\s+");
			   
	int JoinType=0;
	JoinType =Integer.parseInt(subWords[subWords.length-1]);
			   
	int rowNum1 = Integer.parseInt(words[1]);
	int relationIndex1 = Integer.parseInt(words[2]);
	double x11 = Double.parseDouble(words[3]);
	double y11 = Double.parseDouble(words[4]);
	double x12 = Double.parseDouble(words[5]);
	double y12 = Double.parseDouble(words[6]);
	//Rectangle R1 = new Rectangle(rowNum1, relationIndex1,x11,y11,x12,y12);
	int rowNum2 = Integer.parseInt(words[7]);
	int relationIndex2 = Integer.parseInt(words[8]);
	double x21 = Double.parseDouble(words[9]);
	double y21 = Double.parseDouble(words[10]);
	double x22 = Double.parseDouble(words[11]);
	double y22 = Double.parseDouble(words[12]);
	//Rectangle R2 = new Rectangle(rowNum2, relationIndex2,x21,y21,x22,y22);
	JoinTuple jointuple = new JoinTuple(JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22);
	//  System.out.println("Before Red2" + jointuple.toString());
	
	if(JoinType==1) {
            //int numBTuples = Integer.parseInt(context.getConfiguration().get("BCount"));
            //int numTuplesPerReducer = numBTuples/reducersPerRow;
            int reducerRow=rowNum2%reducersPerRow;
            for(int i=0;i<reducersPerRow;i++)
            {
                int reducerIndex=(reducerRow*reducersPerRow)+i;
		context.write(new LongWritable(reducerIndex),jointuple);
            }
	}
	else if(JoinType==2) {
            int reducerRow=rowNum1%reducersPerCol;
            int reducerCol=rowNum2%reducersPerRow;
            int reducerIndex=(reducerRow*reducersPerRow)+reducerCol;
            context.write(new LongWritable(reducerIndex),jointuple);
	}
	else if(JoinType==3) {
            int reducerCol=rowNum1%reducersPerCol;
            for(int i=0;i<reducersPerCol;i++)
            {
                int reducerIndex=(reducersPerRow*i)+reducerCol;
                //                  System.out.println("Writing "+ jointuple.toString() + " to "+ reducerIndex);
                context.write(new LongWritable(reducerIndex),jointuple);
            }
	}
    }
}