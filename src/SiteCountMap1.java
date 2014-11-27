import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

public class SiteCountMap1 extends
		Mapper<LongWritable, WritableWarcRecord, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	// protected void setup(Context cont) {
	// System.err.println(">>>Processing>>> "
	// + ((FileSplit) cont.getInputSplit()).getPath().toString());
	//
	// }

	public void map(LongWritable key, WritableWarcRecord value, Context cont)
			throws IOException, InterruptedException {
		WarcRecord val = value.getRecord();

		String url = val.getHeaderMetadataItem("WARC-Target-URI");
		// if (url != null) {

		String contents = val.getContentUTF8();
		StringTokenizer tokenizer = new StringTokenizer(contents);
		String w;
		try {
			while (tokenizer.hasMoreTokens()) {
				w = url.concat("---");
				w = w.concat(tokenizer.nextToken());
				// word.set(tokenizer.nextToken());
				word.set(w);
				cont.write(word, one);
			}
		} catch (Exception e) {
			System.out.println("****************************Deu merda :(");
		}
		// }

		// try {
		// word.set(new URL(url).getHost());
		// cont.write(word, one);
		// } catch (Exception e) {
		// }
	}
}