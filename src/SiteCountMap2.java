import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SiteCountMap2 extends
		Mapper<LongWritable, Text, Text, Text> {
//	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	

	public void map(LongWritable key, Text value, Context cont)
			throws IOException, InterruptedException {
		
		String[] contents = value.toString().split("---");
		String temp_word = contents[0];
		String url = contents[1];
		
		try {
			word.set(temp_word);
			cont.write(word, new Text(url));
		} catch (Exception e) {
		}
	}
}