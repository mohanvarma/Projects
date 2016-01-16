import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlayerCount {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		private String userId = new String();
		private String userIdW = new String();
		private String userIdB = new String();

		private final static LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				//System.out.println(line);
				if(line.equals("[White") || line.equals("[Black") || line.equals("[Result")) {
					String user;
					String result;

					if(line.contains("White") && !line.contains("+")) {
						user = itr.nextToken();
						user = user.substring(1, user.length()-2);

						// Parse Error:
						if (user.length() < 1) {
							continue;
						}

						//word.set(user+"W");
						//context.write(word, one);
						word.set(user+"+W+Count");
						context.write(word, one);
						userId += "W";
						userIdW = user;
					}
					else if(line.contains("Black") && !line.contains("+")) {
						user = itr.nextToken();
						user = user.substring(1, user.length()-2);

						// Parse Error:
						if (user.length() < 1) {
							continue;
						}

						//word.set(user+"W");
						//context.write(word, one);
						word.set(user+"+B+Count");
						context.write(word, one);
						if(userId.endsWith("W")) {
							userId += "B";
							userIdB = user;
						}
					}
					else if(line.contains("Result")) {
						//System.out.println("Result"+userId);
						user = itr.nextToken();
						user = user.substring(1, user.length()-2);
						//System.out.println(user);
						// Parse Error:
						if (user.length() < 3) {
							//System.out.println("Error");
							continue;
						}

						//word.set(user+"W");
						//context.write(word, one);
						String[] parts = user.split("-");
						//System.out.println(parts[0]+parts[1]);

						// Parse Error
						if (parts.length != 2) {
							continue;
						}

						// We got the result
						if(userId.endsWith("WB")) {
							if(parts[0].equals("1")) {
								//White won
								word.set(userIdW+"+W+Win");
								context.write(word, one);
								word.set(userIdB+"+B+Lost");
								context.write(word, one);
							} else if(parts[0].equals("0")) {
								//Black won
								word.set(userIdB+"+B+Win");
								context.write(word, one);
								word.set(userIdW+"+W+Lost");
								context.write(word, one);
							} else if(parts[0].equals("1/2")) {
								//Draw
								word.set(userIdB+"+B+Draw");
								context.write(word, one);
								word.set(userIdW+"+W+Draw");
								context.write(word, one);
							} else {
								// Reset all
								userId = "";
								userIdW = "";
								userIdB = "";
								continue;
							}
						} else {
							// Reset all
							userId = "";
							userIdW = "";
							userIdB = "";
							continue;
						}

						// Reset all
						userId = "";
						userIdW = "";
						userIdB = "";
					}
				}

			}
		}
	}

	public static class IntSumCombiner
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

	public static class IntSumReducer
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
		Job job = Job.getInstance(conf, "Player Count");
		job.setJarByClass(PlayerCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
