import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class SiteCountReduce2 extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context cont)
			throws IOException, InterruptedException {
		String sum = key.toString();

		for (Text val : values)
			sum += "\n	" + val.toString();

		result.set(sum);

		cont.write(result, new Text());
	}
}