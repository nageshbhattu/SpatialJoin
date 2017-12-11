package emsmain;

//import JoinTuple;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperTwo extends Mapper<LongWritable, Text,LongWritable , JoinTupleNew>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();

        int reducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
	int reducersPerCol = conf.getInt("p1NumOfReducersPerRow", 0);
        
      //  int cellNumber =(int) key.get();
       // int cellRow = cellNumber/reducersPerRow;
       // int cellCol = cellNumber%reducersPerRow;
        String words[] = value.toString().split(",");
	//String[] subWords = words[0].split("\\s+");
			   
	//int count = Integer.parseInt(words[1]);
	int JoinType =Integer.parseInt(words[1]);
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
   //     System.out.println("Join Tyyyyyype*********************\t"+JoinType);
	//Rectangle R2 = new Rectangle(rowNum2, relationIndex2,x21,y21,x22,y22);
	JoinTupleNew jointuple = new JoinTupleNew(JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22);
	 
        switch (JoinType) {
            case 1:
                {
                    int reducerRow=rowNum2%reducersPerRow;
                    for(int i=0;i<reducersPerRow;i++)
                    {
                        int reducerIndex=(reducerRow*reducersPerRow)+i;
                      //  System.out.println("Type1111111\t"+reducerIndex);
                        context.write(new LongWritable(reducerIndex),jointuple);
                    }       break;
                }
            case 2:
                {
                    int reducerRow=rowNum1%reducersPerCol;
                    int reducerCol=rowNum2%reducersPerRow;
                    int reducerIndex=(reducerRow*reducersPerRow)+reducerCol;
                  //  System.out.println("Tyep22222222222\t"+reducerIndex);
                    context.write(new LongWritable(reducerIndex),jointuple);
                    break;
                }
            case 3:
                {
                    int reducerCol=rowNum1%reducersPerCol;
                    for(int i=0;i<reducersPerCol;i++)
                    {
                        int reducerIndex=(reducersPerRow*i)+reducerCol;
                     //   System.out.println("Type3333333\t"+reducerIndex);
                        context.write(new LongWritable(reducerIndex),jointuple);
                    }       break;
                }
            default:
                break;
        }
        
    }
}