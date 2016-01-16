import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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

public class ChessReduce {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		private LongWritable one; // = new LongWritable(1);
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
	extends Reducer<Text,LongWritable,Text,Text> {

		private Map<String, Long> strIntMap = new HashMap<String, Long>();
		private double counter = -1;

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			//System.out.println("*************************Reduce called*********************");
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			//System.out.println("*************Reduced:"+sum);
			strIntMap.put(new String(key.toString()), new Long(sum));
			//System.out.println(key.toString());

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			double average = 0.0;
			Iterator ite = strIntMap.entrySet().iterator();
			//System.out.println("***************************cleanup called*********************");

			if(counter == -1){
				counter = (double)(strIntMap.get("Count"));
				//System.out.println(counter);
				strIntMap.remove("Count");
			}

			Iterator<Entry<String, Long>> it = strIntMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				average = new Double(pair.getValue().toString())/counter;
				String avg = Double.toString(average);
				Text avgToText = new Text(avg);
				Text keyToText = new Text(pair.getKey().toString()+ " " + pair.getValue().toString());
				//System.out.println(avgToText.toString());
				//System.out.println(keyToText.toString());
				context.write(keyToText, avgToText);
				it.remove();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Chess Reduce");
		job.setJarByClass(ChessReduce.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

