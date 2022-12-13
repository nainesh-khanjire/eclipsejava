package weather;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce {
	public static class MaxTemperatureReducer extends

			Reducer<Text, Text, Text, Text> {

		public void reduce(Text Key, Iterator<Text> Values, Context context) throws IOException, InterruptedException {

			String temperature = Values.next().toString();
			context.write(Key, new Text(temperature));
		}

	}

}