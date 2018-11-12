import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;
import org.apdplat.word.dictionary.DictionaryFactory;

public class InvertedIndex{
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
    	private Text word = new Text();
    	private Text value = new Text();

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException{
			//System.out.println(line);
			String[] content = line.toString().split("\\s+");

			//Grab the words in titles, url, stock code
			List<Word> splitedWords = WordSegmenter.seg(content[4].replaceAll("[a-zA-Z0-9]", ""));
			String url = content[content.length-1];
			String code = content[0];
		
			//Send the result to Reducer like <(word, stock code),(1, url)>
			for(Word val : splitedWords){
				word.set(val.getText()+","+code);
				value.set("1"+","+url);
				context.write(word, value);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      		int sum = 0;
      		String urls = "";
      		String oldKey = key.toString();

      		//Caculate the frequent of the word shows.
      		//Gathering the urls
      		for (Text val : values) {
        		String value = val.toString();
        		sum += Integer.parseInt(value.split(",")[0]);
        		if (urls.length() == 0)
        			urls = urls + value.split(",")[1];
        		else
        			urls = value.split(",")[1] + "," + urls;
      		}

      		Text newKey = new Text();
      		Text newValue = new Text();

      		newKey.set(oldKey.split(",")[0]+","+String.valueOf(sum));
      		newValue.set(oldKey.split(",")[1]+","+urls);

      		//Send the result to temp file like <(word, frequent),(stock code,url0,url1,url2,...,urln)>
      		context.write(newKey, newValue);
    	}
	}

	/*
	Create personlized Comparator
	Override the 'compare()' method
	排序规则：
	只比较相同的词语在不同股票新闻中的出现次数
	根据词频，由高到低进行排序
	*/
	public static class TextDecreasingComparator extends WritableComparator{
    	public TextDecreasingComparator(){
        	super(Text.class, true);
    	}

    	public int compare(WritableComparable a, WritableComparable b){
    		String thisText = a.toString();
			String thatText = b.toString();
			String thisKey = thisText.split(",")[0];
			String thatKey = thatText.split(",")[0];
			int thisValue = Integer.parseInt(thisText.split(",")[1]);
			int thatValue = Integer.parseInt(thatText.split(",")[1]);

			if (thisKey.equals(thatKey) || thisKey.compareTo(thatKey) == 0){
				if (thisValue < thatValue)
					return 1;
				else if (thisValue == thatValue)
					return 0;
				else if (thisValue > thatValue)
					return -1;
			}
			else
				return -thisKey.compareTo(thatKey);

			return 1;
    	}
    }

	public static void main(String args[]) throws Exception{
		Path tempDir = new Path("invertedidex-temp-"+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		Configuration conf = new Configuration();

		Job invertedIndexJob = new Job(conf, "invertedIndexJob");
		invertedIndexJob.setJarByClass(InvertedIndex.class);
		try{
			invertedIndexJob.setMapperClass(Map.class);
			invertedIndexJob.setReducerClass(Reduce.class);
			invertedIndexJob.setMapOutputKeyClass(Text.class);
			invertedIndexJob.setMapOutputValueClass(Text.class);
			invertedIndexJob.setOutputKeyClass(Text.class);
			invertedIndexJob.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(invertedIndexJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(invertedIndexJob, tempDir);

			//invertedIndexJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			//Start a new job to do sorting
			if (invertedIndexJob.waitForCompletion(true)){
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(InvertedIndex.class);
				FileInputFormat.addInputPath(sortJob, tempDir);
				//sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				sortJob.setMapperClass(Sort.SortMapper.class);
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
				sortJob.setMapOutputKeyClass(Text.class);
				sortJob.setMapOutputValueClass(Text.class);
				//Set the personalized Comparator
				sortJob.setSortComparatorClass(TextDecreasingComparator.class);
				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally{
			FileSystem.get(conf).deleteOnExit(tempDir);
		}

		System.out.println("Finished!");
	}
}