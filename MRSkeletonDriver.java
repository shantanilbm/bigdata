import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MRSkeletonDriver {

	public static class MyMapper extends Mapper < LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			//MSBL  DP logic
			
			// Mok, Mov extract
			// write newke, newv to context   contex.write(mynewk, mynewv)
		}
	}
	
	public static class MyReducer extends Reducer <Text, Text, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)
		{
			//RSBL  ABL
			// rok, rov
			// write it into context
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MR skeleton for batch227");
		
		// Mapper Reducer driver class
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MRSkeletonDriver.class);
		
		// mok, mov, rokk, rov types
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// input and output format
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// input and output location
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0: 1);

	}

}
