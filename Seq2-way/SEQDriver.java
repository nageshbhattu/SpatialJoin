package abcSequential;
import Common.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SEQDriver 
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        int gridMax = 100000;
        int p1NumOfReducersPerRow = 8;
        int p1NumOfReducers = 64;
        double cellWidth = (double) gridMax / p1NumOfReducersPerRow;

        conf.setInt("gridMax", gridMax);
        conf.setInt("p1NumOfReducersPerRow", p1NumOfReducersPerRow);
        conf.setInt("p1NumOfReducers", p1NumOfReducers);
        conf.setDouble("cellWidth", cellWidth);
        Job job = Job.getInstance(conf, "Join");
        job.setNumReduceTasks(64);

        job.setJarByClass(SEQDriver.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
            TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, Map1.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Rectangle.class);

        job.setReducerClass(Reduce1.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        if (success) {
            //  Configuration conf2 = new Configuration();
            Job job1 = new Job(conf);
            job1.setNumReduceTasks(64);
            job1.setJarByClass(SEQDriver.class);
            conf.set("ACount", "6");
            conf.set("BCount", "6");
            conf.set("CCount", "6");
            conf.set("DCount", "6");

            MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/"+args[2]),
                TextInputFormat.class, Map2.class);
            MultipleInputs.addInputPath(job1, new Path(args[3]),
               TextInputFormat.class, Map2.class);
        
            job1.setReducerClass(Reduce2.class);
         
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(ABJoinTuple.class);

	    job1.setOutputKeyClass(NullWritable.class);
	    job1.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job1, new Path(args[4]));
            success = job1.waitForCompletion(true);
        }
    }
}