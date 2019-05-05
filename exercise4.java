import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class exercise4 extends Configured implements Tool {
	
	
	public static class Map extends Mapper<Object, Text, Text, FloatWritable> {
		private Text artist = new Text();
		private FloatWritable duration = new FloatWritable(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			
			// get the name of artist, which is the key
			artist.set(line[2].toLowerCase());

			//get the duration of the song. which is the value
			duration.set(Float.parseFloat(line[3]));
			context.write(artist, duration);
			
			}
		}
	
	public static class MyPartitioner extends Partitioner<Text, FloatWritable> {
	
			public int getPartition(Text key, FloatWritable value, int numOfReducers) {

				// to sort names, need to change all alphabets to lowercase
				String str = key.toString().replaceAll("\\s+","").toLowerCase();
				
				// 1st group, all artists whose name starts with a number
				if(Character.isDigit(str.charAt(0))) {
					return 0;
				}
				// 2nd group, all artists whose name starts with a letter between a and g
				else if(str.charAt(0) >= 'a' && str.charAt(0) <= 'g'){
					return 1;
				}
				// 3rd group, all artists whose name starts with a letter between h and m
				else if(str.charAt(0) > 'g' && str.charAt(0) <= 'm'){
					return 2;
				}
				// 4th group, all artists whose name starts with a letter between m and s
				else if(str.charAt(0) > 'm' && str.charAt(0) <= 's'){
					return 3;
				}
				// 5th, all artists whose name starts with a letter after s 
				else{
					return 4;
				}
			}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float max_duration = 0;
			for(FloatWritable value: values) {
					if(value.get() > max_duration) {
						max_duration = value.get();
					}
			}
			context.write(key, new FloatWritable(max_duration));
		}
		
	}
	
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Job job = new Job(conf, "exercise4");
		job.setJarByClass(exercise4.class);
		

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(MyPartitioner.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setNumReduceTasks(5);


		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise4(), args);
		System.exit(res);
	}
}


