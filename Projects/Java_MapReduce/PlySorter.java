import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlySorter {

	public static class TokenizerMapper
	extends Mapper<Object, Text, DoubleWritable, Text>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				//Moves
				String moves = itr.nextToken().toString();
				//Frequency
				double freq = Double.parseDouble(itr.nextToken().toString());
				DoubleWritable dble = new DoubleWritable(freq);
				word.set(moves);
				context.write(dble, word);
			}
		}
	}
	
	public static class CustomPartitioner extends Partitioner<DoubleWritable, Text>{
		@Override
		public int getPartition(DoubleWritable db, Text key, int numPartitions)
		{
			// < 0.1/64 - Partition1
			// < 0.1/32 - Partition2
			// < 0.1/8 - Partition3
			// < 0.1/2 - Partition4
			// Rest    - Partition5
			//System.out.println(key);
			Double pct = db.get();
			
			if(pct < 0.1/8) {
				return 0;
			}
			else if(pct < 0.1/4) {
				return 1;
			}
			else if(pct < 0.1/2) {
				return 2;
			}
			else if(pct < 0.1) {
				return 3;
			}
			else
				return 4;
		}
	}

	public static class DoubleSumReducer
	extends Reducer<DoubleWritable,Text,Text,Text> {

		public void reduce(DoubleWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {

			Text txt = new Text();
			Text percentage = new Text();
			for (Text val : values) {
				txt = val;
				String pct = key.toString();
				pct += "%";
				percentage.set(pct);
				context.write(txt, percentage);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Ply Sorter");
		job.setJarByClass(PlySorter.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(DoubleSumReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setNumReduceTasks(5);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

