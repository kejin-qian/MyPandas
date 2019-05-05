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


public class exercise3 extends Configured implements Tool {
	
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text k = new Text("");
		private Text v = new Text(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split(",");

			// check if the year data is erronous
			if(row[165].matches("\\d{4}") &&
						// take the data between 2000 and 2010 (inclusive)
						Integer.parseInt(row[165]) <= 2010 && 
						Integer.parseInt(lirowne[165]) >= 2000) {
				// get song title, artist name, and duration
				v.set(row[1] + ", " + row[2] + ", " + row[3]);
				
				context.write(k, v);
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "exercise3");
		job.setJarByClass(exercise3.class);
		
		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise3(), args);
		System.exit(res);
	}
	
}
