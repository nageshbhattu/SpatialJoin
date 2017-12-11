package emsmain;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RectangleReducerTwo extends Reducer<LongWritable,JoinTupleNew,LongWritable,Text> 
{
    long startr2;
    @Override
    protected void setup(Context context) throws IOException{
        startr2 = System.currentTimeMillis();
    }
    @Override
    public void reduce(LongWritable key, Iterable<JoinTupleNew> value,Context context) throws IOException, InterruptedException 
    {
        TreeSet<JoinTupleNew> lab = new TreeSet<>();
        TreeSet<JoinTupleNew> lbc = new TreeSet<>();
        TreeSet<JoinTupleNew> lcd = new TreeSet<>();   
        int key1 = (int) key.get();
     
        for(JoinTupleNew jt:value)
        {
            JoinTupleNew t = new JoinTupleNew(jt);
            switch (t.JoinType) {
                case 1:
                    lab.add(t);
                    break;
                case 2:
                    lbc.add(t);
                    break;
                case 3:
                    lcd.add(t);
                    break;
                default:
                    break;
            }
        }
       /*     Iterator<JoinTupleNew> itrab = lab.iterator();
            for(int i = 0;i<lab.size();i++){
                System.out.println("lab\t"+ key1+"\t"+itrab.next());
            }
            Iterator<JoinTupleNew> itrbc = lbc.iterator();
            for(int i = 0;i<lbc.size();i++){
                System.out.println("lbc\t"+ key1+"\t"+ itrbc.next());   
            }
            Iterator<JoinTupleNew> itrcd = lcd.iterator();
            for(int i = 0;i<lcd.size();i++){
                   System.out.println("lcd\t"+ key1+"\t"+ itrcd.next());
            }
            System.out.println("BYEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n"); */
        Iterator<JoinTupleNew> itrbc = lbc.iterator();
        while(itrbc.hasNext()) {
            JoinTupleNew lbcTuple = itrbc.next();
            Iterator<JoinTupleNew> itrab = lab.iterator();
            while(itrab.hasNext()) {
                JoinTupleNew labTuple = itrab.next();
                if(labTuple.r2RowNum==lbcTuple.r1RowNum) {
                    Iterator<JoinTupleNew> itrcd = lcd.iterator();
                    while(itrcd.hasNext()) {
                        JoinTupleNew lcdTuple = itrcd.next();
                        if(lbcTuple.r2RowNum==lcdTuple.r1RowNum){

                            String output= labTuple.r1RowNum + "," + lbcTuple.r1RowNum+","+
                                            lbcTuple.r2RowNum + "," + lcdTuple.r2RowNum;
                            Text Result=new Text(output);
                            context.write(new LongWritable(key1), Result);
                        }
                    }
                }
            }
        }  
    }
    
    @Override
    protected void cleanup(Context context) throws IOException{
        startr2 = System.currentTimeMillis() - startr2;
        System.out.println("-------------------------------------------");
        System.out.println("EMSmain R2 IDr1 :" + context.getTaskAttemptID().getTaskID()+ "Reducer Time--:"+startr2);
        System.out.println("-------------------------------------------");
    }
}