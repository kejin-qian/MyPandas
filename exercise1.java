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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class exercise1 extends Configured implements Tool {
	// create a class called Pair
	// will be used to store the following two things: 
	// 1. counts = 1  
	// 2. volumes = The number of volumes/books containing a word (or pairs of words) 

	public static class Pair implements Writable{
		private int counts; 
		private int volumes;
		
		public Pair() {
			this.counts = 0;
			this.volumes = 0;
		}
		public Pair(int counts, int volumes) {
			this.counts = counts;
			this.volumes = volumes;
		}
		public void set(int counts, int volumes) {
			this.counts = counts;
			this.volumes = volumes;
		}
		public int getCounts() {
			return counts;
		}
		public int getVolumes() {
			return volumes;
		}
		public void readFields(DataInput in) throws IOException {
				counts = in.readInt();
				volumes = in.readInt();
		}
		public void write(DataOutput out) throws IOException {
				out.writeInt(volumes);
				out.writeInt(counts);
		} 

	}

	public static class Map1 extends Mapper<Object, Text, Text, Pair> {

		private Text k = new Text();
		private Pair v = new Pair();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");
			String[] keystring = {"nu", "chi", "haw"};

			// Check the existence of each substring
			for(String substring : keystring){
				   if(line[0].toLowerCase().contains(substring)){
					   if(line[1].matches("\\d{4}")) {
						   String sub = line[1] + "," + substring + ",";
						   int counts = 1;
						   int volumes = Integer.parseInt(line[3]);
						   k.set(sub);
						   v.set(counts, volumes);
						   context.write(k, v);
					   }
				   }
				}
		}
	}
	
	public static class Map2 extends Mapper<Object, Text, Text, Pair> {

		private Text k = new Text();
		private Pair v = new Pair();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");
			String[] keystring = {"nu", "chi", "haw"};
			

			for(String substring : keystring){
				   // Check the existence of each substring for the 1st word
					if(line[0].toLowerCase().contains(substring)){
						if(line[2].matches("\\d{4}")) {
							String sub = line[2] + "," + substring + ",";
							int counts = 1;
							int volumes = Integer.parseInt(line[4]);

							k.set(sub);
							v.set(counts, volumes);
							context.write(k, v);
						}
					}
					// Check the existence of each substring for the 2nd word
					if(line[1].toLowerCase().contains(substring)){
						if(line[2].matches("\\d{4}")) {
							String sub = line[2] + "," + substring + ",";
							int counts = 1;
							int volumes = Integer.parseInt(line[4]);
						   
							k.set(sub);
							v.set(counts, volumes);
							context.write(k, v);
					   }
				   }
				}
		}
	}

	public static class Combine extends Reducer<Text, Pair, Text, Pair> {

		public void reduce(Text key, Iterable<Pair> keystring, Context context) throws IOException, InterruptedException {
			int counts = 0;
			int volumes = 0;
			Pair combo = new Pair();
			for(Pair value: keystring) {
					counts += value.getCounts();
					volumes += value.getVolumes();
			}
			combo.set(counts, volumes);
			context.write(key, combo);
		}

	}
	
	public static class Reduce extends Reducer<Text, Pair, Text, FloatWritable> {

		public void reduce(Text key, Iterable<Pair> keystring, Context context) throws IOException, InterruptedException {
			int counts = 0;
			int volumes = 0;
			for(Pair value: keystring) {
					counts += value.getCounts();
					volumes += value.getVolumes();
		}
			float average = (float)volumes/counts;
			context.write(key, new FloatWritable(average));
		}
		
	}
	
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = new Job(conf, "exercise1");
		job.setJarByClass(exercise1.class);

		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);


		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise1(), args);
		System.exit(res);
	}
}