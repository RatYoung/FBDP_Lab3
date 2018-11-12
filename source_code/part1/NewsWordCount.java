import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.apdplat.word.dictionary.DictionaryFactory;


public class NewsWordCount{
	public static class NewsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			System.out.println(line);
			//In order to remove all the alphabets and numbers in news titles
			String[] content = line.toString().replaceAll("[a-zA-Z0-9]", "").split("\\s+");
			List<Word> splitedWords = WordSegmenter.seg(content[4]);
		
			//Send the result to Reducer as <word, 1>
			for(Word value : splitedWords){
				word.set(value.getText());
				//System.out.println(word);
				context.write(word, one);
			}
		}
	}

	public static class NewsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      		int sum = 0;
      		for (IntWritable val : values) {
        		sum += val.get();
      		}
      		if (sum > 10){
      			result.set(sum);
      			context.write(key, result);
      		}
    	}
    }

    //Create a Comparator to sort and override the 'compare()' mathod
    public static class IntWritableDecreasingComparator extends IntWritable.Comparator{
    	public int compare (WritableComparable a, WritableComparable b){
    		return -super.compare(a, b);
    	}

    	public int compare (byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
    		return -super.compare(b1, s1, l1, b2, s2, l2);
    	}
    }

	public static void main(String[] args) throws Exception{
		//String jars = args[2].split(",");
		//WordConfTools.set("dic.path", "classpath:dic.txt, /home/ratyoung/Desktop/material/chi_words.txt");
		//WordConfTools.set("stopwords.path", "classpath:stopwords.txt, /home/ratyoung/Desktop/material/stopwords.txt");		
		//DictionaryFactory.reload();

		//Generate a temp dir path
		Path tempDir = new Path("wordcount-temp-"+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		Configuration conf = new Configuration();

		Job newsJob = new Job(conf, "newsJob");
		newsJob.setJarByClass(NewsWordCount.class);
		try{
			newsJob.setMapperClass(NewsMapper.class);
			newsJob.setReducerClass(NewsReducer.class);
			newsJob.setMapOutputKeyClass(Text.class);
			newsJob.setMapOutputValueClass(IntWritable.class);
			newsJob.setOutputKeyClass(Text.class);
			newsJob.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(newsJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(newsJob, tempDir);

			newsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

			//Do a job one more time to sort the results
			if (newsJob.waitForCompletion(true)){
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(NewsWordCount.class);
				FileInputFormat.addInputPath(sortJob, tempDir);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				sortJob.setMapperClass(InverseMapper.class);
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				//Set the Comparator class to do sorting
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally{
			FileSystem.get(conf).deleteOnExit(tempDir);
		}

		System.out.println("Finished!");
	}
}