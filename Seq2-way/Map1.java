package abcSequential;
import Common.*;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Map1 extends Mapper<LongWritable, Text, LongWritable, Rectangle> 
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
        
        int max = conf.getInt("gridMax",0);
        int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
        double cellWidth =  (double) max / numOfReducersPerRow;
        double cellHeight =  (double) max / numOfReducersPerRow;
        
        String[] line = value.toString().trim().split(",");
        int r1RowNum = Integer.parseInt(line[0]);
        int relationIndex = Integer.parseInt(line[1]);
        double x11 = Double.parseDouble(line[2]);
        double y11 = Double.parseDouble(line[3]);
        double x12 = Double.parseDouble(line[4]);
        double y12 = Double.parseDouble(line[5]);
       
        Rectangle r = new Rectangle(r1RowNum, relationIndex, x11, y11, x12, y12);

        int x1 = (int) Math.floor(x11 / cellWidth);
        int x2 = (int) Math.floor(x12 / cellWidth);
        int y1 = (int) Math.floor(y11 / cellHeight);
        int y2 = (int) Math.floor(y12 / cellHeight);

        for (int j = y1; j <= y2; j++) {
            for (int i = x1; i <= x2; i++) {
                int mapkey = ((j * numOfReducersPerRow) + i);
                context.write(new LongWritable(mapkey), r);
            }
        }
    }
}