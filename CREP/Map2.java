package abcdCREPv2;
import Common.*;
import abcdCREPv2.CREPDriver.MyCounters;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, LongWritable, Rectangle>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);   // 5
        double x11; double x12; double y11; double y12; 
        int rowNum1;    int relationIndex1;
      
        String words[] = value.toString().split(",");
        System.out.println("MAP1OUT"+value.toString());
       // String[] subWords = words[0].split("\\s+");
       
        int cellNumber =Integer.parseInt(words[0].trim());
       // System.out.println("cellnumber:: "+ cellNumber);
        //int cellNumber =(int) key.get();    // or int cellNumber =Integer.parseInt(subWords[0]);
        int cellRow = cellNumber/cellsPerRow;
        int cellCol = cellNumber%cellsPerRow;
        int i,j;    int key2;
       
        int crossed = Integer.parseInt(words[7]);
        rowNum1 = Integer.parseInt(words[1]);
        relationIndex1 = Integer.parseInt(words[2]);
        x11 = Double.parseDouble(words[3]);
        y11 = Double.parseDouble(words[4]);
        x12 = Double.parseDouble(words[5]);
        y12 = Double.parseDouble(words[6]);
        
        Rectangle jt = new Rectangle(rowNum1, relationIndex1, x11, y11, x12, y12);
    /*    if(crossed == 0){
            key2 = (cellRow * cellsPerRow) + cellCol;
            context.write(new LongWritable(key2), jt); 
            context.getCounter(MyCounters.AFTER_REPLICATION).increment(1);
        }
        else{ 
*/          context.getCounter(MyCounters.REPLICATED).increment(1);
            for(i=cellRow; i<cellsPerRow; i++){
                for(j=cellCol; j<cellsPerRow; j++){
                    key2 = (i*cellsPerRow) + j;
                    context.getCounter(MyCounters.AFTER_REPLICATION).increment(1);
                    // System.out.println(rowNum1+"keyyyyyyyyyyyyyy\t"+key2);
                    context.write(new LongWritable(key2), jt); 
                }
            }
    //    }
    } 
}