import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Sort{
	public static class SortMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text word = new Text();
		private Text value = new Text();

		//Read the temp results to be prepared to do sorting
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			String[] content = line.toString().split("\\s+");
			word.set(content[0]);
			value.set(content[1]);
			context.write(word, value);
		}
	}
}