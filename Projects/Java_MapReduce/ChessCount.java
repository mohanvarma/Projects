import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessCount {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		//private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				//System.out.println(line);
				if(line.contains("Result")){
					line = itr.nextToken();
					//System.out.println(line);
					// Parse Error:
					if (line.charAt(line.length()-1) != ']') {
						continue;
					}
					line = line.substring(0, line.length()-1);
					String[] parts = line.split("-");

					//Parse Error:
					if (parts.length != 2) {
						continue;
					}

					//Parse Error:
					if (parts[0].charAt(0) != '"' || parts[1].charAt(parts[1].length()-1) != '"') {
						continue;
					}
					//System.out.println(parts[0]);
					String first = parts[0].substring(1, parts[0].length());
					//System.out.println(parts[1]);
					String second = parts[1].substring(0, parts[1].length()-1);

					//Parse Error:
					if (first.length() == 0 || second.length() == 0) {
						continue;
					}

					LongWritable wScore = new LongWritable(0);
					LongWritable bScore = new LongWritable(0);
					LongWritable zero = new LongWritable(0);
					LongWritable one = new LongWritable(1);
					LongWritable half = new LongWritable(1);

					word.set("Count");
					context.write(word, one);

					if(first.equals("1"))
					{
						wScore = one;
						word.set("White");
						context.write(word, wScore);
						continue;
					}
					else if(first.equals("0"))
					{
						wScore = zero;
					}
					else if(first.equals("1/2"))
					{
						wScore = half;
						word.set("Draw");
						context.write(word, wScore);
						continue;
					}


					if(second.equals("1"))
					{
						bScore = one;
						word.set("Black");
						context.write(word, bScore);
						continue;
					}
					else if(second.equals("0"))
					{
						bScore = zero;
					}
					else if(second.equals("1/2"))
					{
						bScore = half;
					}

				}
			}
		}
	}

	public static class LongSumCombiner
	extends Reducer<Text,LongWritable,Text,LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class LongSumReducer
	extends Reducer<Text,LongWritable,Text,LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class CustomPartitioner extends Partitioner<Text, LongWritable>{
		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions)
		{
			int partition = 0;
			//System.out.println(key);
			if(key.toString().equals("White")) {
				return 0;
			}
			else if(key.toString().equals("Black")) {
				return 1;
			}
			else if(key.toString().equals("Draw")) {
				return 2;
			}
			else if(key.toString().equals("Count")) {
				return 3;
			}
			return partition;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Chess Count");
		job.setJarByClass(ChessCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumCombiner.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setReducerClass(LongSumReducer.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
