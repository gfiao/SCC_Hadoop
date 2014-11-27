/**
 *  SiteCount.java - counts the number of pages each site has (the number
 *  of times each hostname appears at document header)
 *  usage example of edu.cmu.lemurproject.* InputFormat and RecordReader
 *  vad@fct.unl.pt - 2014 / SCC
 *  based on public WordCount examples
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;

import edu.cmu.lemurproject.*;
import java.net.URL;

public class SiteCount {

	public static void main(String[] args) throws Exception {

		//*******************MAP-REDUCE 1**************************

		Job conf = Job.getInstance(new Configuration(), "sitecount");
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

		//TODO

	}
}
