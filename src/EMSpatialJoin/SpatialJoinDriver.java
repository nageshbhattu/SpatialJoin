package EMSpatialJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SpatialJoinDriver 
{
	public static void main(String[] args) throws Exception 
	{
	    Configuration conf = new Configuration();
	    int gridMax = 100000;
            int p1NumOfReducersPerRow = 5;
            int p1NumOfReducers = 25;
            double cellWidth = (double) gridMax / p1NumOfReducersPerRow;
            String multiple = "multiple";
            conf.set(multiple, multiple);
       //     int startTime= 00;
            conf.setInt("gridMax", gridMax);
            conf.setInt("p1NumOfReducersPerRow", p1NumOfReducersPerRow);
            conf.setInt("p1NumOfReducers", p1NumOfReducers);
            conf.setDouble("cellWidth", cellWidth);
            Job job = Job.getInstance(conf, "Join");
	    job.setNumReduceTasks(25);
            
	    job.setJarByClass(SpatialJoinDriver.class);
	    job.setMapperClass(RectangleMapper.class);
	    //job.setCombinerClass(RectangleReducer.class);
	    job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Rectangle.class);

	    job.setReducerClass(RectangleReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
        //    job.getTaskCompletionEvents(startTime);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   // System.exit(job.waitForCompletion(true) ? 0 : 1);
      //     MultipleOutputs.addNamedOutput(job, multiple, TextOutputFormat.class, LongWritable.class, Text.class);
	 
	    boolean success = job.waitForCompletion(true);
        if (success) {
        	Job job1 = new Job(conf);
        	job1.setJarByClass(RectangleMapperTwo.class);
                conf.set("ACount", "6");
        	conf.set("BCount", "6");
        	conf.set("CCount", "6");
        	conf.set("DCount", "6");
                
            FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/"+args[1]+"/"+"multiple*"));
            
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            //Job job1 = Job.getInstance(conf, "JOB_1");
            job1.setMapperClass(RectangleMapperTwo.class);
            job1.setReducerClass(RectangleReducerTwo.class);
            //job1.setInputFormatClass(KeyValueTextInputFormat.class);
            
            job1.setPartitionerClass(SpatialPartitioner.class);
            job1.setNumReduceTasks(25);
            // FileOutputFormat.setOutputPath(job1, new Path(outputFinalDir));
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(JoinTuple.class);
            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Text.class);
            
            success = job1.waitForCompletion(true);
        }
      //  return success?0:1;	  
    }
	
}	