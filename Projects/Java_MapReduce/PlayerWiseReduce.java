import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlayerWiseReduce {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		//private final static LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String str = itr.nextToken();
				long lng = Long.parseLong(str);
				context.write(word, new LongWritable(lng));
			}
		}
	}
	
	public static class CustomPartitioner extends Partitioner<Text, LongWritable>{
		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions)
		{
			// [aA]-[eE] - Partition1
			// [fF]-[jJ] - Partition2
			// [kK]-[oO] - Partition3
			// [pP]-[tT] - Partition4
			// Rest      - Partition5
			//System.out.println(key);
			if (key.toString() == "") {
				return 4;
			}
			char ch = key.toString().charAt(0);
			ch = Character.toLowerCase(ch);
			
			if(ch >= 'a' && ch <= 'e') {
				return 0;
			}
			else if(ch >= 'f' && ch <= 'j') {
				return 1;
			}
			else if(ch >= 'k' && ch <= 'o') {
				return 2;
			}
			else if(ch >= 'p' && ch <= 't') {
				return 3;
			}
			else
				return 4;
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,LongWritable,Text,Text> {
		private LongWritable result = new LongWritable();
		// Total count played as white
		long w_count = 0;
		double w_wins = 0;
		double w_lost = 0;
		double w_draw = 0;
		long b_count = 0;
		double b_wins = 0;
		double b_lost = 0;
		double b_draw = 0;
		
		String text = new String("");
		String format = new String("");

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {

			
			for (LongWritable val : values) {
				//System.out.println(key.toString()+":"+val.get());// + key.toString().substring(0, key.toString().length()-8));
				if (key.toString().contains("+B+Count")) {
					//If text contains a valid string, write it
					if(text.length() > 0) {
						Text txt = new Text("");
						// Append vals
						//text += Double.toString(b_wins);
						//text += " ";
						//text += Double.toString(b_lost);
						//text += " ";
						//text += Double.toString(b_draw);
						
						//System.out.println("Format:"+format);
						if(format.equals("D")) {
							String parts[] = text.split(" ");
							if(parts.length != 3)
								continue;
							//Split will return 3;
							text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[2]+" "+"0.0";
						} else if(format.equals("DL")) {
							String parts[] = text.split(" ");
							if(parts.length != 4)
								continue;
							//Split will return 4;
							text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[3]+" "+parts[2];
						} else if(format.equals("L")) {
							String parts[] = text.split(" ");
							if(parts.length != 3)
								continue;
							//Split will return 3;
							text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[2]+" 0.0";
						} else if(format.equals("W")) {
							String parts[] = text.split(" ");
							if(parts.length != 3)
								continue;
							//Split will return 3;
							text = parts[0]+" "+parts[1]+" "+parts[2]+" 0.0 0.0";
						} else if(format.equals("DW")) {
							String parts[] = text.split(" ");
							if(parts.length != 4)
								continue;
							text =parts[0]+" " +parts[1]+ " "+parts[3]+" 0.0 "+parts[2];
						} else if(format.equals("LW")) {
							String parts[] = text.split(" ");
							if(parts.length != 4)
								continue;
							text =parts[0]+" " +parts[1]+ " "+parts[3]+" "+parts[2]+" 0.0";
						}
						else if(format.equals("DLW")) {
							String parts[] = text.split(" ");
							if(parts.length != 5)
								continue;
							//Split will have 5
							text =parts[0]+" " +parts[1]+ " "+parts[4]+" "+parts[3]+" "+parts[2];
						}
						context.write(new Text(text), txt);
						
						//Reset
						text = "";
						format = "";
						b_wins = 0;
						b_draw = 0;
						b_lost = 0;
						
						w_wins = 0;
						w_draw = 0;
						w_lost = 0;
						
						text += key.toString().substring(0, key.toString().length()-8)+" Black";
						b_count = val.get();
					} else {
						// Otherwise, first time,
						text += key.toString().substring(0, key.toString().length()-8)+" Black";
						b_count = val.get();
					}
				} else if(key.toString().contains("+B+Draw")) {
					//Compute pct
					double pct = (double)val.get()/b_count;
					//b_draw = pct;
					//System.out.println("Draw:"+pct+":"+val.get()+":"+b_count);
					String str = " "+Double.toString(pct);
					text += str;
					format += "D";
				} else if(key.toString().contains("+B+Lost")) {
					//Compute pct
					double pct = (double)val.get()/b_count;
					//System.out.println("Lost:"+pct+":"+val.get()+":"+b_count);
					String str = " "+Double.toString(pct);
					text += str;
					//b_lost = pct;
					format += "L";
				} else if(key.toString().contains("+B+Win")) {
					//Compute pct
					double pct = (double)val.get()/b_count;
					String str = " "+Double.toString(pct);
					text += str;
					//b_wins = pct;
					format += "W";
				}else if (key.toString().contains("+W+Count")) {
					//If text contains a valid string, write it
					if(text.length() > 0) {
						Text txt = new Text("");
						// Append vals
						//text += Double.toString(w_wins);
						//text += " ";
						//text += Double.toString(w_lost);
						//text += " ";
						//text += Double.toString(w_draw);
						/*
						if(format.equals("D")) {
							text += " 0.0 0.0";
						} else if(format.equals("DL")) {
							text += " 0.0";
						} else if(format.equals("DLW")) {
							;
						}
						*/
						if(format.equals("D")) {
							String parts[] = text.split(" ");
							if(parts.length != 3)
								continue;
							//Split will return 3;
							text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[2]+" "+"0.0";
						} else if(format.equals("DL")) {
							String parts[] = text.split(" ");
							if(parts.length != 4)
								continue;
							//Split will return 4;
							text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[3]+" "+parts[2];
						} else if(format.equals("L")) {
							String parts[] = text.split(" ");
							if(parts.length != 3)
								continue;
							//Split will return 3;
							text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[2]+" 0.0";
						} else if(format.equals("W")) {
							String parts[] = text.split(" ");
							if(parts.length != 3)
								continue;
							//Split will return 3;
							text = parts[0]+" "+parts[1]+" "+parts[2]+" 0.0 0.0";
						} else if(format.equals("DW")) {
							String parts[] = text.split(" ");
							if(parts.length != 4)
								continue;
							text =parts[0]+" " +parts[1]+ " "+parts[3]+" 0.0 "+parts[2];
						} else if(format.equals("LW")) {
							String parts[] = text.split(" ");
							if(parts.length != 4)
								continue;
							text =parts[0]+" " +parts[1]+ " "+parts[3]+" "+parts[2]+" 0.0";
						}
						else if(format.equals("DLW")) {
							String parts[] = text.split(" ");
							if(parts.length != 5)
								continue;
							//Split will have 5
							text =parts[0]+" " +parts[1]+ " "+parts[4]+" "+parts[3]+" "+parts[2];
						}
						context.write(new Text(text), txt);
						
						//Reset
						text = "";
						format = "";
						b_wins = 0;
						b_draw = 0;
						b_lost = 0;
						
						w_wins = 0;
						w_draw = 0;
						w_lost = 0;
						
						text += key.toString().substring(0, key.toString().length()-8)+" White";
						w_count = val.get();
					}else {
						// Otherwise, first time,
						text += key.toString().substring(0, key.toString().length()-8)+" White";
						w_count = val.get();
					}
				} else if(key.toString().contains("+W+Draw")) {
					//Compute pct
					double pct = (double)val.get()/w_count;
					w_draw = pct;
					String str = " "+Double.toString(pct);
					text += str;
					format += "D";
				} else if(key.toString().contains("+W+Lost")) {
					//Compute pct
					double pct = (double)val.get()/w_count;
					String str = " "+Double.toString(pct);
					text += str;
					w_lost = pct;
					format += "L";
				} else if(key.toString().contains("+W+Win")) {
					//Compute pct
					double pct = (double)val.get()/w_count;
					String str = " "+Double.toString(pct);
					text += str;
					w_wins = pct;
					format += "W";
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(format.equals("D")) {
				String parts[] = text.split(" ");
				if(parts.length != 3)
					return;
				//Split will return 3;
				text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[2]+" "+"0.0";
			} else if(format.equals("DL")) {
				String parts[] = text.split(" ");
				if(parts.length != 4)
					return;
				//Split will return 4;
				text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[3]+" "+parts[2];
			} else if(format.equals("L")) {
				String parts[] = text.split(" ");
				if(parts.length != 3)
					return;
				//Split will return 3;
				text = parts[0]+" "+parts[1]+" "+"0.0 "+parts[2]+" 0.0";
			} else if(format.equals("W")) {
				String parts[] = text.split(" ");
				if(parts.length != 3)
					return;
				//Split will return 3;
				text = parts[0]+" "+parts[1]+" "+parts[2]+" 0.0 0.0";
			} else if(format.equals("DW")) {
				String parts[] = text.split(" ");
				if(parts.length != 4)
					return;
				text =parts[0]+" " +parts[1]+ " "+parts[3]+" 0.0 "+parts[2];
			} else if(format.equals("LW")) {
				String parts[] = text.split(" ");
				if(parts.length != 4)
					return;
				text =parts[0]+" " +parts[1]+ " "+parts[3]+" "+parts[2]+" 0.0";
			}
			else if(format.equals("DLW")) {
				String parts[] = text.split(" ");
				if(parts.length != 5)
					return;
				//Split will have 5
				text =parts[0]+" " +parts[1]+ " "+parts[4]+" "+parts[3]+" "+parts[2];
				;
			}
			context.write(new Text(text), new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PlayerWise Reducer");
		job.setJarByClass(PlayerWiseReduce.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setNumReduceTasks(5);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
