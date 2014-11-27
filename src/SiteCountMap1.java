public class SiteCountMap1 extends
	Mapper<LongWritable, WritableWarcRecord, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	protected void setup(Context cont) {
		System.err.println(">>>Processing>>> "
				+ ((FileSplit) cont.getInputSplit()).getPath().toString());

	}

	public void map(LongWritable key, WritableWarcRecord value, Context cont)
			throws IOException, InterruptedException {
		WarcRecord val = value.getRecord();

		String url = val.getHeaderMetadataItem("WARC-Target-URI");
		try {
			word.set(new URL(url).getHost());
			cont.write(word, one);
		} catch (Exception e) {
		}
	}
}