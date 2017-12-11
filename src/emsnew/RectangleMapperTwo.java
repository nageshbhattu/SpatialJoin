package emsnew;

import ControlledRepSpatialJoin.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RectangleMapperTwo extends Mapper<LongWritable, Text, LongWritable, Rectangle>
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        int cellsPerRow = conf.getInt("p1NumOfReducersPerRow",0);   // 5
        
        double x11; double x12; double y11; double y12; 
        int rowNum1;    int relationIndex1;
      
        String words[] = value.toString().split(",");
       // String[] subWords = words[0].split("\\s+");
       
        int cellNumber =Integer.parseInt(words[0].trim());
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
        
        Rectangle jt = new Rectangle(crossed, rowNum1, relationIndex1, x11, y11, x12, y12);
        if(crossed == 0){
            key2 = (cellRow * cellsPerRow) + cellCol;
            context.write(new LongWritable(key2), jt);  
            
        }
        else{  
           // System.out.println("Crosseeeeeeeeedddddddddddddddddd\t"+crossed);
            for(i=cellRow; i<cellsPerRow; i++){
                for(j=cellCol; j<cellsPerRow; j++){
                    key2 = (i*cellsPerRow) + j;
                //    System.out.println("keyyyyyyyyyyyyyy\t"+key2);
                    context.write(new LongWritable(key2), jt); 
                    
                }
            }
        }
    } 
}