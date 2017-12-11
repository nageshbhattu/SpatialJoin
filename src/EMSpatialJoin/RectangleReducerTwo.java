package EMSpatialJoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RectangleReducerTwo extends Reducer<LongWritable,JoinTuple,LongWritable,Text> 
{
    @Override
    public void reduce(LongWritable key, Iterable<JoinTuple> value,Context context) throws IOException, InterruptedException 
    {
       // int amountOfJoin = 0;
        ArrayList<JoinTuple> lab = new ArrayList<>();
        ArrayList<JoinTuple> lbc = new ArrayList<>();
        ArrayList<JoinTuple> lcd = new ArrayList<>();
     //   int key1 = (int) key.get();
            for(JoinTuple jt:value)
            {
           //     System.out.println(jt.toString());
                JoinTuple t = new JoinTuple(jt);
             //   System.out.println("REDUCER TWO\t"+key.get()+"\t"+ t);
                    if(t.JoinType==1) {
                        lab.add(t);
                    }
                    else if (t.JoinType==2) {
                        lbc.add(t);
                    }
                    else if(t.JoinType==3) {
                        lcd.add(t);
                    }
            }
            for(int i=0;i<lbc.size();i++) {
		JoinTuple lbcTuple = lbc.get(i);
                    for(int j=0;j<lab.size();j++) {
		    	JoinTuple labTuple = lab.get(j);
                            if(labTuple.r2RowNum==lbcTuple.r1RowNum) {
		    		for(int k=0;k<lcd.size();k++) {
                                    JoinTuple lcdTuple = lcd.get(k);
		    			if(lbcTuple.r2RowNum==lcdTuple.r1RowNum) {
                                            //  int x=key1;
                                         //   amountOfJoin++;
                                            String output= labTuple.r1RowNum + "," + lbcTuple.r1RowNum+","+
                                                            lbcTuple.r2RowNum + "," + lcdTuple.r2RowNum;
		    						//key=new LongWritable(x);
                                            Text Result=new Text(output);
                                            context.write(key, Result);
		    			}
                                }
                            }
                    }
            }
          //  context.write(key, new Text(amountOfJoin));
    }
}