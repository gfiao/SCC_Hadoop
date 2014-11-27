import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class SiteCountReduce2 extends
	Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context cont)
			throws IOException, InterruptedException {
		String sum = key.toString()+"\n";
		
//		System.out.println("*********************** KEY -> " + key.toString());
		
		for (Text val : values) {
//			System.out.println("------------------------ VAL -> " + val.toString());
			
			sum += "	"+val.toString()+"\n";
		}
		result.set(sum);
		
//		System.out.println("++++++++++++++++++++++\n" + sum + "\n++++++++++++++++++++++++++++");
		
		cont.write(result, new Text());
	}
}