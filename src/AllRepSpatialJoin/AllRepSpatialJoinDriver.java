package AllRepSpatialJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AllRepSpatialJoinDriver 
{
    public static int main(String[] args) throws Exception 
    {   
        Configuration conf = new Configuration();
        int gridMax = 100000;
        int p1NumOfReducersPerRow = 5;
        int p1NumOfReducers = 25;
        
        String allRep;
        allRep = "allRep";
        conf.set("allRep", allRep);
     //   int p1NumOfReducersPerCol = p1NumOfReducers / p1NumOfReducersPerRow;
        double cellWidth = (double) gridMax / p1NumOfReducersPerRow;
        conf.setInt("gridMax", gridMax);
        conf.setInt("p1NumOfReducersPerRow", p1NumOfReducersPerRow);
        //    conf.setInt("p1NumOfReducersPerCol", p1NumOfReducersPerCol);
        conf.setInt("p1NumOfReducers", p1NumOfReducers);
        conf.setDouble("cellWidth", cellWidth);
          
       // conf.setString("allRep", allRep);
        Job job = Job.getInstance(conf, "Join");
	job.setNumReduceTasks(100);
            
	job.setJarByClass(AllRepSpatialJoinDriver.class);
	job.setMapperClass(RectangleMapper.class);
	//job.setCombinerClass(RectangleReducer.class);
	job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Rectangle.class);

	job.setReducerClass(RectangleReducer.class);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        MultipleOutputs.addNamedOutput(job, allRep, TextOutputFormat.class , LongWritable.class, Text.class);
        // System.exit(job.waitForCompletion(true) ? 0 : 1);
        boolean success = job.waitForCompletion(true);
        return success?0:1;	
    }       
}	