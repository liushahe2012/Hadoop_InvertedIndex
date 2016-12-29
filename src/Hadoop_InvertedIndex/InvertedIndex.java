package Hadoop_InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {

	public static class Map extends Mapper<Object, Text, Text, Text>
	{
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private FileSplit split;
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//简单起见，只获取文件名
			split = (FileSplit)context.getInputSplit();
			int index = split.getPath().toString().indexOf("file");
			String fileName = split.getPath().toString().substring(index);
			
			StringTokenizer st = new StringTokenizer(value.toString());
			while(st.hasMoreTokens())
			{
				//单词：文件名  作为key
				//1 为 value
				keyInfo.set(st.nextToken()+":"+fileName);
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
			}
		}
	}
	
	//combine
	public static class Combine extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			// 统计词频
			int sum = 0;
			for(Text temp : values)
			{
				sum+= Integer.parseInt(temp.toString());
			}
			
			int index = key.toString().indexOf(":");
			// 重新设置key值为单词
			String sKey = key.toString().substring(0, index);
			Text outKey = new Text(sKey);
			
			// 重新设置value值由URL和词频组成
			String sValue = key.toString().substring(index+1);
			Text outValue = new Text(sValue+":"+sum);
			
			context.write(outKey, outValue);
		}
	}
	
	//reduce
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			String fileList = new String();
			for(Text temp : values)
			{
				fileList += temp.toString() + ";";						
			}
			
			context.write(key, new Text(fileList));
		}
	}
	
	//main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: InvertedIndex <in>  <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
