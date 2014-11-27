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

		String contents = val.getContentUTF8();
		StringTokenizer tokenizer = new StringTokenizer(contents,
				SiteCount.patterns);
		String w;
		try {
			while (tokenizer.hasMoreTokens()) {
				String curr_token = tokenizer.nextToken();

				// ignora palavras com menos de 3 letras
				if (curr_token.length() > 2) {
					w = curr_token.toLowerCase().concat("---").concat(url);
//					w = w.concat();

					word.set(w);
					cont.write(word, one);
				}
			}
		} catch (Exception e) {
			System.out.println("****************************Deu merda :(");
		}
	}
}