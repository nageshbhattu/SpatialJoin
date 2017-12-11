package ControlledRepSpatialJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SpatialPartitioner extends 
    Partitioner<LongWritable, Rectangle> {
	@Override
	public int getPartition(LongWritable key, Rectangle value, int numberOfPartitions) {
            long num = key.get();
            return Math.abs((int) (num % numberOfPartitions));
	}
}