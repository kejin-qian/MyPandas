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


public class exercise2 extends Configured implements Tool {

	public static class Pair implements Writable{
		private double count; 
		private double volume;
		private double sqvolume; 
		
		public Pair() {
			// initialize count, volume and squared volume
			this.count = 0;
			this.volume = 0;
			this.sqvolume = 0; 
		}
		public Pair(double count, double volume, double sqvolume) {
			this.count = count;
			this.volume = volume;
			this.sqvolume = sqvolume; 
		}
		public void set(double count, double volume, double sqvolume) {
			this.count = count;
			this.volume = volume;
			this.sqvolume = sqvolume; 
		}
		public double getCount() {
			return count;
		}
		public double getVolume() {
			return volume;
		}
		public double getSquareVolume() {
			return sqvolume;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
				count = in.readDouble();
				volume = in.readDouble();
				sqvolume = in.readDouble();
		}

		@Override
		public void write(DataOutput out) throws IOException {
				out.writeDouble(count);
				out.writeDouble(volume);
				out.writeDouble(sqvolume);
		} 
	}

	// Mapper for one-gram data
	public static class Map1 extends Mapper<Object, Text, Text, Pair> {
			// Set key to whatever
			private Text k = new Text("");
			// Initialize output value
			private Pair v = new Pair(); 
			
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String[] line = value.toString().split("\\s+");
				
				// get the number of volumes of the current line
				double volume = Double.parseDouble(line[3]);

				// count is 1, because 1 volume in each line
				double count = 1; 

				// sum of squares for sd calculation
				double sqvolume = volume * volume; 

				// the output value is a Pair of count, volume and ss
				v.set(count, volume, sqvolume);
				context.write(k, v);
			}
	}
	
	// Mapper for two-gram data
	public static class Map2 extends Mapper<Object, Text, Text, Pair> {
		// Set key to ""
		private Text k = new Text("");
		// Set value to a pair of sum, count, sum square
		private Pair v = new Pair(); 

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");

			// two-gram data, so volume is at index 4
			double volume = Double.parseDouble(line[4]);
			double count = 1; 
			double sqvolume = volume * volume; 

			v.set(count, volume, sqvolume);
			context.write(k, v);
		}
	}


	// create a Reducer
	public static class Combine extends Reducer<Text, Pair, Text, Pair> {

		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			double count = 0;
			double sum = 0;
			double ss = 0;
			// Initialize the output value for reducer
			Pair combo = new Pair();
			
			// output value for reducer is a Pair of the total number of counts(number of lines), the sum of all volumes and the sum of squares
			for(Pair value: values) {
					count += value.getCount();
					sum += value.getVolume();
					ss += value.getSquareVolume();
			}
			combo.set(count, sum, ss);
			context.write(key, combo);
		}

	}
	
	public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			double n = 0;
			double sum = 0;
			double ss = 0;
			for(Pair value: values) {
					n += value.getCount();
					sum += value.getVolume();
					ss += value.getSquareVolume();
			}
			// calculate mean volume
			double mean = sum/n; 
			// calculate variance
			double variance = (1/n) * (ss - (n*mean*mean));
			// sd is the square root of variance
			double sd = Math.sqrt(variance);
			context.write(key, new DoubleWritable(sd));

		}
		
	}
	
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
	

		Job job = new Job(conf, "exercise2");
		job.setJarByClass(exercise2.class);
		

		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
	

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);


		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise2(), args);
		System.exit(res);
	}
	
}
