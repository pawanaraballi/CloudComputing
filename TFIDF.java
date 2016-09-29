import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static final String FILECOUNT = "count";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args); // getting the instance of
														// the class and
														// initializing the run
														// function
		System.exit(res);
	}

	// 6 parameters
	// Note: Each line in hadoop is a document

	public int run(String[] args) throws Exception {

		Job jobTF = Job.getInstance(getConf(), " term frequency "); // creating
																	// the job
																	// to
		// hadoop
		jobTF.setJarByClass(this.getClass()); // creates a jar file
		FileInputFormat.addInputPaths(jobTF,
				args[0]); // Read input : Path of
														// the
		// input file in HDFS
		// FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(jobTF, new Path(args[0]+"intermediate")); // Output file
		// path in HDFS
		jobTF.setMapperClass(MapTF.class); // Mapper class initialized to
											// execute
		// the map function
		jobTF.setReducerClass(ReduceTF.class); // Reducer class initialized to
		// execute the reduce function
		jobTF.setOutputKeyClass(Text.class); // The Key-value pair - The key is
		// being defined as String up here
		// with Text.class
		jobTF.setOutputValueClass(FloatWritable.class); // The Key-Value pair -
		// The value part of it
		// is being defined up
		// here as FloatWritable

		jobTF.waitForCompletion(true);

		FileSystem fileSystem = FileSystem.get(getConf());
		ContentSummary contentSummary = fileSystem.getContentSummary(new Path(
				args[0]));
		getConf().set(FILECOUNT, contentSummary.getFileCount() + "");

		Job job = Job.getInstance(getConf(), " TFIDF "); // creating the job to
															// hadoop
		job.setJarByClass(this.getClass()); // creates a jar file
		// 6 parameters
		// Different types of input and output formats
		FileInputFormat.addInputPaths(job, args[0] + "intermediate"); // Read input : Path of the
														// input file in HDFS
		// FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output file
																// path in HDFS
		job.setMapperClass(Map.class); // Mapper class initialized to execute
										// the map function
		job.setReducerClass(Reduce.class); // Reducer class initialized to
											// execute the reduce function
		job.setOutputKeyClass(Text.class); // The Key-value pair - The key is
											// being defined as String up here
											// with Text.class
		job.setOutputValueClass(Text.class); // The Key-Value pair -
														// The value part of it
														// is being defined up
														// here as FloatWritable

		return job.waitForCompletion(true) ? 0 : 1; // returns true if it is
													// successful with all the
													// statistics
	}

	public static class Map extends
 Mapper<LongWritable, Text, Text, Text> {
		// LongWritable is the offset to determine the line in the document :
		// LongWriteable is the key as each line is considered as a document
		// Text is the value for the entire document - that is a line
		// 3rd parameter the out key - basically a text now
		// 4th parameter is the associated value to it
		private Text word = new Text();
		// public static final Log log = (Log) LogFactory.getLog(Map.class);

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// context of the map

			String line = lineText.toString();
			ArrayList<Text> keyWord = new ArrayList<>();
			ArrayList<Text> valueWord = new ArrayList<>();
			Text currentWord = new Text();
			Text currValueWord = new Text();
			if(line.isEmpty()){
				return;
			}
			String[] splittingdata = line.toString().split("&#&#&");
			if(splittingdata.length < 0){
				return;
			}
			String[] splitagain = splittingdata[1].split("\t");
			String newvalue = splitagain[0] + "=" + splitagain[1];
			currentWord = new Text(splittingdata[0]);
			currValueWord = new Text(newvalue);
			context.write(currentWord, currValueWord);	
		}
	}

	public static class Reduce extends
 Reducer<Text, Text, Text, Text> {
		// 1st and 2nd parameter is the same datatype from the mapper
		// 3rd and 4th parameter is the final output with the key - value pair
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {
			long filecount = context.getConfiguration().getLong(FILECOUNT, 1); // Assigning
																				// the
																				// default
																				// values
			float idf = 0;
			ArrayList<Text> filesCountWithWords = new ArrayList<>();
			for (Text filecountwithword : counts) {
				filesCountWithWords.add(new Text(filecountwithword.toString()));
			}

			for (Text files : filesCountWithWords) {
				String[] filenameWithTF = files.toString().split("=");
				double tfidftest = 0;
				tfidftest = Double.parseDouble(filenameWithTF[1])
						* Math.log10(1 + (filecount / filesCountWithWords
								.size()));
				String delimitWithfilename = word.toString() + "&#&#&"
						+ filenameWithTF[0];
				Text tempword = new Text(delimitWithfilename);
				context.write(tempword, new Text(tfidftest + ""));
			}
		}
	}
	public static class MapTF extends
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
                        	String x = word.toString() + delimeter + filename + ".txt";
				currentWord = new Text(x);
				context.write(currentWord, one);
				// Assigning all the different words with default value of 1 
				// (hadoop,1)
			}
		}
	}

	public static class ReduceTF extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		// 1st and 2nd parameter is the same datatype from the mapper
		// 3rd and 4th parameter is the final output with the key - value pair
		@Override
		public void reduce(Text word, Iterable<FloatWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			float wf = 0;

			for (FloatWritable count : counts) {
				sum += count.get();
			}
			wf = (float) (1 + (Math.log(sum) / Math.log(10)));
			context.write(word, new FloatWritable(wf));
		}
	}

}
