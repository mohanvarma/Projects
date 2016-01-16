import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlyCount {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		private final static LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				//System.out.println(line);
				if(line.contains("PlyCount")){
					String count = itr.nextToken();
					
					// Parse Error:
					if (count.charAt(count.length()-1) != ']') {
						continue;
					}
					
					count = count.substring(0, count.length()-1);
					
					//System.out.println("PlyCount"+count+count.charAt(0)+count.charAt(count.length()-1));

					// Parse Error:
					if (count.charAt(0) != '\"' || count.charAt(count.length()-1) != '\"') {
						//System.out.println("Parse Error1");
						continue;
					}

					// Parse Error:
					if (count.length() <= 2) {
						//System.out.println("Parse Error2");
						continue;
					}

					
					count = count.substring(1, count.length()-1);
					//System.out.println("PlyCount"+count);
					//int intCount = Integer.parseInt(count);
					//count = Integer.toString(intCount);
					word.set(count);
					context.write(word, one);
					word.set("Count");
					context.write(word, one);
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Ply Count");
		job.setJarByClass(PlyCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumCombiner.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

