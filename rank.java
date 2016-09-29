import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;
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

// Author : Pawan Araballi
// Email : paraball@uncc.edu
// Student ID : 800935601

public class rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(rank.class);
	private static final String FILECOUNT = "count";
	private static final String NOOFARGS = "noofargs";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new rank(), args); 
		System.exit(res);
	}

	// 6 parameters
	// Note: Each line in hadoop is a document

	public int run(String[] args) throws Exception {


		String[] noofargs = new String[args.length-2]; // declaring a array of string with length of args -2 as the first is the path of input and output
		for(int i = 2; i < args.length; i++){
			noofargs[i-2] = args[i];
		}

		FileSystem fileSystem = FileSystem.get(getConf());
		ContentSummary contentSummary = fileSystem.getContentSummary(new Path(
				args[0]));
		getConf().set(FILECOUNT, contentSummary.getFileCount() + "");
		
		Job jobTF = Job.getInstance(getConf(), " term frequency "); 
		jobTF.setJarByClass(this.getClass()); 
		FileInputFormat.addInputPaths(jobTF,
				args[0]); 
		FileOutputFormat.setOutputPath(jobTF, new Path(args[0]+"intermediate")); // output to a intermediate folder
		jobTF.setMapperClass(MapTF.class); 
		jobTF.setReducerClass(ReduceTF.class); 
		jobTF.setOutputKeyClass(Text.class); 
		jobTF.setOutputValueClass(FloatWritable.class); 
		jobTF.waitForCompletion(true); // wait till the job is completed

		
		Job jobTFIDF = Job.getInstance(getConf(), " TFIDF "); 
		jobTFIDF.setJarByClass(this.getClass()); 
		FileInputFormat.addInputPaths(jobTFIDF, args[0] + "intermediate"); // reading the intermediate file
		FileOutputFormat.setOutputPath(jobTFIDF, new Path(args[0] + "searchintermediate")); // output to another intermediate file
		jobTFIDF.setMapperClass(MapTFIDF.class); 
		jobTFIDF.setReducerClass(ReduceTFIDF.class); 
		jobTFIDF.setOutputKeyClass(Text.class); 
		jobTFIDF.setOutputValueClass(Text.class); 
		jobTFIDF.waitForCompletion(true); // wait till the job is completed


		Job jobSearch = Job.getInstance(getConf(), " search "); 
		jobSearch.setJarByClass(this.getClass()); 
		FileInputFormat.addInputPaths(jobSearch,
				args[0] + "searchintermediate"); // reading the intermediate file
		
		jobSearch.getConfiguration().setStrings(NOOFARGS,noofargs); //assigning the final variable
		FileOutputFormat.setOutputPath(jobSearch, new Path(args[0] + "rankintermediate")); //output to another intermediate variable
		jobSearch.setMapperClass(MapSearch.class); 
		jobSearch.setReducerClass(ReduceSearch.class); 
		jobSearch.setOutputKeyClass(Text.class); 
		jobSearch.setOutputValueClass(Text.class); 
		jobSearch.waitForCompletion(true); // wait till the job is completed	
		
		Job jobRank = Job.getInstance(getConf(), " rank "); 
		jobRank.setJarByClass(this.getClass()); 
		//jobRank.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		FileInputFormat.addInputPaths(jobRank, args[0] + "rankintermediate"); //reading the intermediate file
		FileOutputFormat.setOutputPath(jobRank, new Path(args[1])); //final output 
		jobRank.setMapperClass(MapRank.class); 
		jobRank.setReducerClass(ReduceRank.class);
		jobRank.setMapOutputKeyClass(FloatWritable.class);
		jobRank.setMapOutputValueClass(Text.class);
		jobRank.setOutputKeyClass(Text.class); 
		jobRank.setOutputValueClass(FloatWritable.class); 
		
		return jobRank.waitForCompletion(true) ? 0 : 1; // success of failure of the program. Gives a small summary
		}

// Map class for Rank

	public static class MapRank extends
 Mapper<LongWritable, Text, FloatWritable, Text> {
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// context of the map

			String line = lineText.toString();
			Text currentWord = new Text();
			Text currValueWord = new Text();
			if (line.isEmpty()) {
				return;
			}
			String[] splittingdata = line.toString().split("\t"); // splitting the data with the delimiter of \t
			if (splittingdata.length < 0) {
				return;
			}
			currentWord = new Text(splittingdata[0]);
			float value = Float.parseFloat(splittingdata[1]) * -1; // exploiting the auto sorting by mapreduce to sort it in descending order
			context.write(new FloatWritable(value),currentWord);
	
		}
	}

//Reduce class for Rank

	public static class ReduceRank extends
 Reducer<FloatWritable, Text, Text, FloatWritable> {
		public void reduce(FloatWritable counts, Iterable<Text> word,
				Context context) throws IOException, InterruptedException {
			float temp = 0;		
			temp = counts.get() * -1;
			String s = "";
			for (Text w : word) {
	        		s = w.toString();
	    		}
			context.write(new Text(s), new FloatWritable(temp));
		
		}
	}

// Rest of the mapper and reducer class is same as implemented previously in search.java TFIDF.java

	public static class MapSearch extends
 Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		// Initial setup to assign the variables in argument to a TreeSet 

		TreeSet<String> keysToSearch;
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			String[] noargs = context.getConfiguration().getStrings(NOOFARGS);
			keysToSearch = new TreeSet<>();
			if(noargs != null){			
				for(int i = 0; i < noargs.length; i++){
					keysToSearch.add(noargs[i]);
				}
			}
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// context of the map

			String line = lineText.toString();
			Text currentWord = new Text();
			Text currValueWord = new Text();
			if (line.isEmpty()) {
				return;
			}
			String[] splittingdata = line.toString().split("&#&#&");
			if (splittingdata.length < 0) {
				return;
			}
			if(keysToSearch.contains(splittingdata[0])){
				String[] splitagain = splittingdata[1].split("\t");
				currentWord = new Text(splitagain[0]);
				currValueWord = new Text(splitagain[1]);
				context.write(currentWord, currValueWord);
			}
	
		}
	}

	public static class ReduceSearch extends
 Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {
						float sum = 0;
		    for (Text count : counts) {
		        sum += Float.parseFloat(count.toString());
		    }
		    Text temp = new Text(sum + "");
		    context.write(word, temp);
		}
	}



	public static class MapTFIDF extends
 Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

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

	public static class ReduceTFIDF extends
 Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {
			long filecount = context.getConfiguration().getLong(FILECOUNT, 1); 
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
		private final static FloatWritable one = new FloatWritable(1);
		private Text word = new Text();
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
			}
		}
	}

	public static class ReduceTF extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
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
