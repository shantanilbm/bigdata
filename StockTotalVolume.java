package com.pavan.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockTotalVolume {



	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			String line = value.toString();// deserialize
			String[] parts = line.split("\\t");
			if(parts.length == 9)
			{
				String stockName = parts[1];
				long volume = Long.valueOf(parts[7]);
				context.write(new Text(stockName), new LongWritable(volume));// serialize write method
			}
		}
	}

	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable value : values)  // cli, 1000, cli, 2000, (cli, 3000)  mok, mov ==> cli, { 1000, 2000, 3000}
			{
				sum = sum + value.get(); 
				// deserialize longwritable 
			}
			context.write(key, new LongWritable(sum));
		}
	} 
	
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Finding the stock Volume");
		job.setJarByClass(StockTotalVolume.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class );
		
		job.setCombinerClass(MyReducer.class);
		
		
		//job.setCombinerClass(MyReducer.class);
		//job.setNumReduceTasks(2);
		//job.setPartitionerClass(MyPartition.class)
		
		//job.setCombinerClass(MyReducer.class)
		//job.setPartitionerClass(MyPartitionClass)
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
