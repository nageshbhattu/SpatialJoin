package GEMS2Latest1;
import Common.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GEMSDriver 
{
    public enum MyCounters{
        ABTYPE, BCTYPE, CDTYPE, ABREPLICATE, CDREPLICATE, TAU_P, TAU_S;
    }
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        int gridMax = 100000;
        int p1NumOfReducersPerRow = 8;
        int p1NumOfReducersPerCol = 8;
        int p1NumOfReducers = 64;
        double cellWidth = (double) gridMax / p1NumOfReducersPerRow;
            
        conf.setInt("gridMax", gridMax);
        conf.setInt("p1NumOfReducersPerRow", p1NumOfReducersPerRow);
        conf.setInt("p1NumOfReducersPerCol", p1NumOfReducersPerCol);
        conf.setInt("p1NumOfReducers", p1NumOfReducers);
        conf.setDouble("cellWidth", cellWidth);
        
        Job job = Job.getInstance(conf, "Join");
        job.setNumReduceTasks(64);

        job.setJarByClass(GEMSDriver.class);
        job.setMapperClass(Map1.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Rectangle.class);

        job.setReducerClass(Reduce1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
        boolean success = job.waitForCompletion(true);
        if (success) 
        {
            Job job1 = new Job(conf);
            job1.setJarByClass(Map2.class);
            conf.set("ACount", "6");
            conf.set("BCount", "6");
            conf.set("CCount", "6");
            conf.set("DCount", "6");
                
            FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/"+args[1]+"/"+"part*"));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            job1.setMapperClass(Map2.class);
            job1.setReducerClass(Reduce2.class);
            job1.setPartitionerClass(SpatialPartitioner.class);
            job1.setNumReduceTasks(64);
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Rectangle.class);
            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Text.class);
            success = job1.waitForCompletion(true);
        }
      //  return success?0:1;	  
    }
}	