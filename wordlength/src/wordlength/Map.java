package wordlength;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

	private final static IntWritable one = new IntWritable();
	private IntWritable wordSize = new IntWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {

			String word = itr.nextToken();
			wordSize.set(word.length());
			context.write(wordSize, one);
		}

	}

}
