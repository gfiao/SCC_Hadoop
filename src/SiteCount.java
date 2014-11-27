/**
 *  SiteCount.java - counts the number of pages each site has (the number
 *  of times each hostname appears at document header)
 *  usage example of edu.cmu.lemurproject.* InputFormat and RecordReader
 *  vad@fct.unl.pt - 2014 / SCC
 *  based on public WordCount examples
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.cmu.lemurproject.WarcFileInputFormat;

public class SiteCount {

	public static void main(String[] args) throws Exception {

		//*******************MAP-REDUCE 1**************************

		Job conf = Job.getInstance(new Configuration(), "sitecount1");
		conf.setJarByClass(SiteCount.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(SiteCountMap1.class);
		conf.setCombinerClass(SiteCountReduce1.class);
		conf.setReducerClass(SiteCountReduce1.class);

		conf.setInputFormatClass(WarcFileInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.waitForCompletion(true); // submit and wait


		//*******************MAP-REDUCE 2**************************

//		Job conf2 = Job.getInstance(new Configuration(), "sitecount2");
//		conf2.setJarByClass(SiteCount.class);
//
//		conf2.setOutputKeyClass(Text.class);
//		conf2.setOutputValueClass(IntWritable.class);
//
//		conf2.setMapperClass(SiteCountMap1.class);
//		conf2.setCombinerClass(SiteCountReduce1.class);
//		conf2.setReducerClass(SiteCountReduce1.class);
//
//		conf2.setInputFormatClass(WarcFileInputFormat.class);
//		conf2.setOutputFormatClass(TextOutputFormat.class);
//
//		FileInputFormat.setInputPaths(conf2, new Path(args[0]));
//		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
//
//		conf2.waitForCompletion(true); // submit and wait
		//TODO

	}
}
