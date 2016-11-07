package MapReduce.Hadoop;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Name: Pawan Araballi
// Student ID: 800935601
// email: paraball@uncc.edu

public class pagerank extends Configured implements Tool {
	private static final String VALUEOFN = "valueofn";
	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner.run( new pagerank(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {
		FileSystem fs = FileSystem.get(getConf());
		Job jobcount  = Job.getInstance(getConf(), "pagerank");
		jobcount.setJarByClass( this.getClass());
		long nvalue;
		
		// Job to calculate the value of N
		FileInputFormat.addInputPaths(jobcount,  args[0]);
		FileOutputFormat.setOutputPath(jobcount,  new Path(args[3]));
		jobcount.setMapperClass( Mapcount.class);
		jobcount.setReducerClass( Reducecount.class);
		jobcount.setMapOutputValueClass(IntWritable.class);
		jobcount.setOutputKeyClass( Text.class);
		jobcount.setOutputValueClass(IntWritable.class);
		jobcount.waitForCompletion(true);
		fs.delete(new Path(args[3]), true);
		
		// N value is calculated
		nvalue = jobcount.getCounters().findCounter("Result", "Result").getValue();

		// Job to get the title and outlinks
		Job job  = Job.getInstance(getConf(), "pagerank");
		job.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(job,  args[0]);
		FileOutputFormat.setOutputPath(job,  new Path(args[1]+"job0"));
		job.getConfiguration().setStrings(VALUEOFN, nvalue + "");
		job.setMapperClass( MapInitial.class);
		job.setReducerClass( ReduceInitial.class);
		job.setOutputKeyClass( Text.class);
		job.setOutputValueClass( Text.class);
		job.waitForCompletion(true);

		int i = 1;
		//Job to calculate the page rank and iterate for 10 iterations     
		for(i=1;i<=10;i++){
			Job job2  = Job.getInstance(getConf(), "pagerank");
			job2.setJarByClass( this.getClass());
			FileInputFormat.addInputPaths(job2,  args[1]+"job"+(i-1));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"job"+i));    
			job2.setMapperClass( MapRank.class);
			job2.setReducerClass( ReduceRank.class);
			job2.setOutputKeyClass( Text.class);
			job2.setOutputValueClass( Text.class);
			job2.waitForCompletion(true);
			fs.delete(new Path(args[1]+"job"+(i-1)), true); 
		}
		
		//job to sort the elements 
		Job job3  = Job.getInstance(getConf(), "pagerank");
		job3.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(job3,  args[1]+"job"+(i-1));
		FileOutputFormat.setOutputPath(job3,  new Path(args[2]));
		job3.setMapperClass( Map3.class);
		job3.setReducerClass( Reduce3.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass( Text.class);
		job3.setOutputValueClass( DoubleWritable.class);
		//fs.delete(new Path(args[1]+"job"+(i-1)), true);
		job3.waitForCompletion(true);
		return 1; // Making sure that it always exits 

	}
	public static class Mapcount extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern title = Pattern
				.compile("<title>(.*?)</title>");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			if (line != null && !line.isEmpty()) {
				Text currentUrl = new Text();

				Matcher matcher1 = title.matcher(line);

				if (matcher1.find()) {
					currentUrl = new Text(matcher1.group(1));
					context.write(new Text(currentUrl), new IntWritable(1));
				}

			}

		}
	}

	public static class Reducecount extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		int counter = 0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException,
				InterruptedException {
			this.counter++;
		}

		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			context.getCounter("Result", "Result").increment(counter);
		}
	}



	public static class MapInitial extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		private static final Pattern title = Pattern.compile("<title>(.*?)</title>");
		private static final Pattern text = Pattern.compile("<text+\\s*[^>]*>(.*?)</text>");
		private static final Pattern outl = Pattern.compile("\\[\\[(.*?)\\]\\]");
		double valueofn;

		public void setup(Context context) throws IOException, InterruptedException{
			valueofn = context.getConfiguration().getDouble(VALUEOFN, 1);
		}


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			if(line != null && !line.isEmpty() ){
				Text url  = new Text();
				Text currlinks  = new Text();
				String links = null;
				Matcher matcher1 = title.matcher(line);
				Matcher matcher2 = text.matcher(line);
				Matcher matcher3 = null;

				if(matcher1.find()){
					url  = new Text(matcher1.group(1));
				}

				if(matcher2.find()){
					links = matcher2.group(1);
					matcher3 = outl.matcher(links);
				}
				double onebyn = (double)1/(valueofn);
				StringBuilder str = new StringBuilder("##"+onebyn+"##");
				int count=1;
				while(matcher3 != null && matcher3.find()) {
					links=matcher3.group(1);
					if(count>1)
						str.append("@!#"+matcher3.group(1));
					else if(count==1){
						str.append(links);
						count++;
						}
				}
				currlinks = new Text(str.toString());
				context.write(url,currlinks);
			}        
		}
	}
// This reducer will not do anything
	public static class ReduceInitial extends Reducer<Text ,  Text ,  Text ,  Text > {
		public void reduce( Text word,  Text counts,  Context context)
				throws IOException,  InterruptedException {
			context.write(word,counts);
		}
	}

	public static class MapRank extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			float rank=0.0f;
			String line  = lineText.toString(),srank="";
			String[] vals=line.split("##");		        
			float initialrank =Float.parseFloat(vals[1]);
			if(vals.length<3){
				context.write(new Text(vals[0]),new Text("##"+vals[1])); 
			}
			else if(vals.length==3){
				String[] olinks=vals[2].split("@!#");
				context.write(new Text(vals[0]),new Text("##"+vals[1]+"##"+vals[2]));
				for(int j=0;j< olinks.length;j++){	
					String pages= olinks[j];
					//calculate ranks for out links
					rank=0.85f * (initialrank/(float)(olinks.length)) ;
					srank=Float.toString(rank);
					pages=pages.replace(" ",""); // replacing the empty space
					//Added newly calculated pagerank
					context.write(new Text(pages+"!*"), new Text("##"+srank));
				}
			}
		}
	}
	public static class ReduceRank extends Reducer<Text ,  Text ,  Text ,  Text > {
		static String temp="";
		static String keyvalue="";
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {			        
			String key=word.toString(),rankstr="";
			String[] arr;
			float rank=0.0f;
			key=key.trim(); // trimming the key 
			//counts has all page ranks as text
			if(!key.contains("!*")){
				if(!keyvalue.equals(""))//not empty
				{
					context.write(new Text(keyvalue), new Text(temp));}
				for(Text value:counts){
					rankstr=value.toString();
				}
				//storing key and temp for next iterations
				keyvalue=key;
				temp=rankstr;
			}
			else{ //calculate the updated cost
				key=key.substring(0,key.length()-2); //storing the key
				key.trim();

				for(Text value:counts){ 
					rankstr=value.toString();
					rankstr=rankstr.substring(2);
					rank+=Float.parseFloat(rankstr);			        		  
				}
				rank+=0.15f;

				// Replacing the rank
				if(key.equals(keyvalue)){
					arr=temp.split("##");
					if(arr.length>2){
						temp="##"+rank+"##"+arr[2]; //
					}
					else{
						temp="##"+rank; // if the key does not have outlinks
					}
					keyvalue="";
					context.write(new Text(key),new Text(temp));
				}
				else{
					if(!keyvalue.equals("")){ 
						context.write(new Text(keyvalue), new Text(temp) ); 
						keyvalue="";
					}
					context.write(new Text(key), new Text("##"+ Float.toString(rank) ) );
				}
			}
		}
	}


	public static class Map3 extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {  
			String line  = lineText.toString();
			String[] array=line.split("##");
			double rank= Double.parseDouble(array[1]);
			context.write(new DoubleWritable(-1 * rank), new Text(array[0])); // Exploting the auto sorting by mapreduce to sort the page rank
		}
	}
	public static class Reduce3 extends
	Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable counts, Iterable<Text> word,
				Context context) throws IOException, InterruptedException {
			double temp = 0;		
			temp = counts.get() * -1; // Again multiplying by -1 to sort it
			String s = "";
			for (Text w : word) {
				s = w.toString();
				context.write(new Text(s), new DoubleWritable(temp));
			}
		}
	}
}
