package Sequential;

import Sequential.SpatialJoinDriverSeq.MyCounters;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleMapperSeqForAB extends Mapper<LongWritable, Text,LongWritable , ABJoinTuple>
{
    long startr1;
    public void setup(Reducer.Context context){
        startr1 = System.currentTimeMillis();
    }
    static String check0 = "check0";
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
           //   System.out.println("Map" + value.toString());
    String words[] = value.toString().split(",");
    String[] subWords = words[0].split("\\s+");
    int abType=1;
    int JoinType=0;
   
    JoinType =Integer.parseInt(subWords[subWords.length-1]);
    int rowNum1 = Integer.parseInt(words[1]);
    int relationIndex1 = Integer.parseInt(words[2]);
    double x11 = Double.parseDouble(words[3]);
    double y11 = Double.parseDouble(words[4]);
    double x12 = Double.parseDouble(words[5]);
    double y12 = Double.parseDouble(words[6]);
	
    int rowNum2 = Integer.parseInt(words[7]);
    int relationIndex2 = Integer.parseInt(words[8]);
    double x21 = Double.parseDouble(words[9]);
    double y21 = Double.parseDouble(words[10]);
    double x22 = Double.parseDouble(words[11]);
    double y22 = Double.parseDouble(words[12]);
  	ABJoinTuple jointuple = new ABJoinTuple(abType,JoinType,rowNum1,relationIndex1,x11,y11,x12,y12,rowNum2,relationIndex2,x21,y21,x22,y22);		 
        Configuration conf = context.getConfiguration();   
        int max = conf.getInt("gridMax",0);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;
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
             //   context.getCounter(MyCounters.AB_COUNT).increment(1);
            }
        }
    }
    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException{
        startr1 = System.currentTimeMillis() - startr1;
        System.out.println("-------------------------------------------");
    //    System.out.println("CRep R1 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr1);
        System.out.println("-------------------------------------------");
    }
}