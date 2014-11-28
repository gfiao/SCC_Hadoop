import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import tp2.Tp2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SiteCountReduce1_2 extends
	Reducer<Text, Text, Text, Text> {

    private int total_occurrences = 0;
	private Text result = new Text();
    private Map<String, Integer> map = new HashMap<String, Integer>();

	public void reduce(Text key, Iterable<Text> values, Context cont)
			throws IOException, InterruptedException {
		String sum = key.toString() +"\n";
		for (Text val : values) {
            Integer i = map.get(val.toString());
			i++;
            map.put(val.toString(), i);
            total_occurrences++;
		}


        for(Map.Entry<String, Integer> e: map.entrySet())
            sum += e.getKey() +" "+ e.getValue() + "\n";



		result.set(sum);

//		cont.write(key, result);
        cont.write(result, new Text());
	}
}