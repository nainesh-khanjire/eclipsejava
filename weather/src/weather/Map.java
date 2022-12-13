package weather;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

	public static final int MISSING = 9999;

	@Override
	public void map(LongWritable arg0, Text Value, Context context) throws IOException, InterruptedException {

		String line = Value.toString();

		if (!(line.length() == 0)) {
			String date = line.substring(6, 14);

			float temp_Max = Float.parseFloat(line.substring(49, 52).trim());

			float temp_Min = Float.parseFloat(line.substring(57, 60).trim());

			if (temp_Max > 30.0) {

				context.write(new Text("The Day is Hot Day :" + date), new Text(String.valueOf(temp_Max)));

			}

			if (temp_Min < 15) {

				context.write(new Text("The Day is Cold Day :" + date), new Text(String.valueOf(temp_Min)));
			}
		}
	}
}