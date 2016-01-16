import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlyReducer {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		private LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String str = itr.nextToken();
				long temp = Long.parseLong(str);
				one = new LongWritable(temp);
				context.write(word, one);
			}
		}
	}

	public static class LongSumReducer
	extends Reducer<Text,LongWritable, Text, DoubleWritable> {
		//private IntWritable result = new IntWritable();
		private Map<String, Long> strIntMap = new HashMap<String, Long>();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			//System.out.println("**************Reduce called*************");
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			//result.set(sum);
			//System.out.println(key.toString()+":"+sum);
			strIntMap.put(new String(key.toString()), new Long(sum));
			//context.write(key, result);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			long counter = strIntMap.get("Count");
			//int counter = 1;
			strIntMap.remove("Count");
			//System.out.println("********Cleanup Called*****************:"+counter);

			Iterator<Entry<String, Long>> it = strIntMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				double average = new Double(pair.getValue().toString())/counter;
				average = average*100.0;
				String avg = Double.toString(average);
				Text avgToText = new Text(avg);
				Text keyToText = new Text(pair.getKey().toString());
				//System.out.println(avgToText.toString());
				//System.out.println(keyToText.toString());
				DoubleWritable dbleWrtble = new DoubleWritable(average);
				context.write(keyToText, dbleWrtble);
				it.remove();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Ply Reducer");
		job.setJarByClass(PlyReducer.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setNumReduceTasks(1);
		//job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		//TODO: Verify the setOutputValueClass
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
