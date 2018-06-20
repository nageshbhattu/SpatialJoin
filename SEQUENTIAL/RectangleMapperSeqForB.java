package Sequential;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapperSeqForB extends Mapper<LongWritable, Text, LongWritable, RectangleSeq> 
{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    int max = conf.getInt("gridMax",0);
    int numOfReducersPerRow = conf.getInt("p1NumOfReducersPerRow", 0);
    double cellWidth =  (double) max / numOfReducersPerRow;
    double cellHeight =  (double) max / numOfReducersPerRow;

    String[] line = value.toString().trim().split(",");
    double[] x = {Double.parseDouble(line[2]), Double.parseDouble(line[4])};
    double[] y = {Double.parseDouble(line[3]), Double.parseDouble(line[5])};
    int relation=0 ;
        if (line[1].equals("B")) 
        {
            relation = 2;
        }
       
        RectangleSeq r = new RectangleSeq(Integer.parseInt(line[0]), relation, x[0], y[0], x[1], y[1]);

        int x1 = (int) Math.floor(x[0] / cellWidth);
        int x2 = (int) Math.floor(x[1] / cellWidth);
        int y1 = (int) Math.floor(y[0] / cellHeight);
        int y2 = (int) Math.floor(y[1] / cellHeight);

        for (int j = y1; j <= y2; j++) {
            for (int i = x1; i <= x2; i++) {
                int mapkey = ((j * numOfReducersPerRow) + i);
            //   System.out.println("MapperB ReducerAB ------->" + mapkey +" \t" + r.toString());
                context.write(new LongWritable(mapkey), r);
            }
        }
    }
}