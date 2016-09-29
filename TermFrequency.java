import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.commons.logging.LogFactory;

// Author : Pawan Araballi
// Email : paraball@uncc.edu
// Student ID : 800935601

public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args); // getting the instance of the class and initializing the run function
		System.exit(res);
	}
	
	// 6 parameters
	// Note: Each line in hadoop is a document

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " termfrequency "); // creating the job to hadoop
		job.setJarByClass(this.getClass()); // creates a jar file 
		// 6 parameters
		//Different types of input and output formats
		FileInputFormat.addInputPaths(job, args[0]); // Read input : Path of the input file in HDFS
		//FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output file path in HDFS
		job.setMapperClass(Map.class); // Mapper class initialized to execute the map function 
		job.setReducerClass(Reduce.class); // Reducer class initialized to execute the reduce function
		job.setOutputKeyClass(Text.class); // The Key-value pair - The key is being defined as String up here with Text.class
		job.setOutputValueClass(FloatWritable.class); // The Key-Value pair - The value part of it is being defined up here as FloatWritable

		return job.waitForCompletion(true) ? 0 : 1; // returns true if it is successful with all the statistics
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		// LongWritable is the offset to determine the line in the document : LongWriteable is the key as each line is considered as a document
		// Text is the value for the entire document - that is a line
		// 3rd parameter the out key - basically a text now
		// 4th parameter is the associated value to it 
		private final static FloatWritable one = new FloatWritable(1);
		private Text word = new Text();
//		public static final Log log = (Log) Logfactory.getLog(Map.class);
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			//context of the map

			String line = lineText.toString();
			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				FileSplit filesplit = (FileSplit)context.getInputSplit();
                        	String filename = filesplit.getPath().getName();
                        	String delimeter = new String("&#&#&");
                        	String x = word.toString() + delimeter + filename + ".txt" + "\t";
				currentWord = new Text(x);
				context.write(currentWord, one);
				// Assigning all the different words with default value of 1 
				// (hadoop,1)
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		//1st and 2nd parameter is the same datatype from the mapper
		//3rd and 4th parameter is the final output with the key - value pair
		@Override
		public void reduce(Text word, Iterable<FloatWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			float wf = 0;
			
			for (FloatWritable count : counts) {
				sum += count.get();
			}
			wf = (float) (1+(Math.log(sum)/Math.log(10)));
			context.write(word, new FloatWritable(wf));
		}
	}
}
