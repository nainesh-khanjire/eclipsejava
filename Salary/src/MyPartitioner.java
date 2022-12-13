import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

class MyPartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		StringTokenizer str = new StringTokenizer(value.toString());
		str.nextToken();
		int age = Integer.parseInt(str.nextToken());
		if (age < 20)
			return 0;
		else if (age > 20 && age < 50)
			return 1 % numReduceTasks;
		return 2 % numReduceTasks;
	}
}