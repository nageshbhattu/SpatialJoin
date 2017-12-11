package emsmain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SpatialPartitioner extends 
   Partitioner<LongWritable, JoinTupleNew> {

	@Override
	public int getPartition(LongWritable key, JoinTupleNew value, int numberOfPartitions) {
		long num = key.get();
		return Math.abs((int) (num % numberOfPartitions));
	}
}