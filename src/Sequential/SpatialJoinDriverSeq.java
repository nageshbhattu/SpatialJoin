package Sequential;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpatialJoinDriverSeq 
{
	public static void main(String[] args) throws Exception
	{
	    Configuration conf = new Configuration();
	    int gridMax = 20000;
            int p1NumOfReducersPerRow = 5;
            int p1NumOfReducers = 25;
            double cellWidth = (double) gridMax / p1NumOfReducersPerRow;
            
            conf.setInt("gridMax", gridMax);
            conf.setInt("p1NumOfReducersPerRow", p1NumOfReducersPerRow);
            conf.setInt("p1NumOfReducers", p1NumOfReducers);
            conf.setDouble("cellWidth", cellWidth);
            Job job = Job.getInstance(conf, "Join");
	    job.setNumReduceTasks(25);
            
	    job.setJarByClass(SpatialJoinDriverSeq.class);
            
            MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, RectangleMapperSeqForA.class);
            MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, RectangleMapperSeqForB.class);
            
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(RectangleSeq.class);

            job.setReducerClass(RectangleReducerSeqAB.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	  
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
	   
	    boolean success = job.waitForCompletion(true);
        if (success) {
          //  Configuration conf2 = new Configuration();
        	Job job1 = new Job(conf);
                job1.setNumReduceTasks(25);
                job1.setJarByClass(SpatialJoinDriverSeq.class);
                conf.set("ACount", "6");
        	conf.set("BCount", "6");
        	conf.set("CCount", "6");
        	conf.set("DCount", "6");
            
            MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/"+args[2]),
                TextInputFormat.class, RectangleMapperSeqForAB.class);
            MultipleInputs.addInputPath(job1, new Path(args[3]),
               TextInputFormat.class, RectangleMapperSeqForC.class);
        
            job1.setReducerClass(RectangleReducerSeqBC.class);
         
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(ABJoinTuple.class);

	    job1.setOutputKeyClass(LongWritable.class);
	    job1.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job1, new Path(args[4]));
	   // success = job1.waitForCompletion(true);
           boolean success1 = job1.waitForCompletion(true);
       
        
          if (success1) {
          //  Configuration conf2 = new Configuration();
        	Job job2 = new Job(conf);
                job2.setNumReduceTasks(25);
                job2.setJarByClass(SpatialJoinDriverSeq.class);
            
            MultipleInputs.addInputPath(job2, new Path("hdfs://localhost:9000/"+args[4]),
                TextInputFormat.class, RectangleMapperSeqForABC.class);
            MultipleInputs.addInputPath(job2, new Path(args[5]),
               TextInputFormat.class, RectangleMapperSeqForD.class);
        
            job2.setReducerClass(RectangleReducerSeqCD.class);
            
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(ABCJoinTuple.class);

	    job2.setOutputKeyClass(LongWritable.class);
	    job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[6]));
            
            success1 = job2.waitForCompletion(true);
            //boolean success2 = job.waitForCompletion(true);
      }
     //  return success?0:1;	  
    }
 }
}