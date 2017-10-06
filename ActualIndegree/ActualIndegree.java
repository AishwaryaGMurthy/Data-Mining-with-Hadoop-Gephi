// cc A MapReduce program to count the number of distinct sender Emails in the Enron dataset provided 
// as a collection of Sequence files
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ActualIndegree extends Configured implements Tool 
{static class IndegreeMapper extends Mapper<Text, BytesWritable, Text, Text> {

	private final Text one = new Text();
	private final Text key = new Text();
			
	private String stripCommand(String line, String com) 
	{
		int len = com.length();
		if (line.length() > len)
		return line.substring(len);
		return null;
	}
			
	private String procFrom(String line) 
	{
		if (line == null)
			return null;
		String[] froms;
		String from = null;
		do 
		{
			froms = line.split("\\s+|,+", 5);
			// This will only include Email accounts originating from the Enron domain
			if (froms.length == 1 && froms[0].matches(".+@.*[Ee][Nn][Rr][Oo][Nn].+"))
				from = froms[0];
			for (int i = 0; i < froms.length - 1; i++) 
			{
				if (froms[i].matches(".+@.+")) 
				{
					from = froms[i];
					break;
				}
			}
			line = froms[froms.length - 1];
		} while (froms.length > 1 && from == null);
		return from;
	}
	
	@Override
	public void setup(Context context) throws IOException,InterruptedException {
	}

	@Override
	public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
								
		byte[] bytes = value.getBytes();
		
		Scanner scanner = new Scanner(new ByteArrayInputStream(bytes), "UTF-8");
		
		String from = null; // Sender email
		
		ArrayList<String> recipients = new ArrayList<String>(); // Recipient list
		String to = null;
		String cc = null;
		String bcc = null;
		
		String timestamp = null; // Date
		
		for (; scanner.hasNext(); ) {
			String line = scanner.nextLine();
			//To get the 'from' mail address
			if (line.startsWith("From:")) {
				from = procFrom(stripCommand(line, "From: "));
			}
			//To get the 'to' mail address
			else if (line.startsWith("To:")) {
				to = procFrom(stripCommand(line, "To: "));
				//recipients.add(to);
				//Checked for null values
				if (to == null)
				{
				}
				else
				{   
					//Added to 'recipients' list
					recipients.add(to);	
				}	
								
			}
			//To get the 'cc' mail address
			else if (line.startsWith("Cc: ")) {
				cc = procFrom(stripCommand(line, "Cc: "));
				//recipients.add(cc);
				//Checked for null values
				if (cc == null)
				{
				}
				else
				{
					//Added to 'recipients' list
					recipients.add(cc);	
				}	
				
			}
			else if (line.startsWith("Bcc: ")) {
				bcc = procFrom(stripCommand(line, "Bcc: "));
				//recipients.add(bcc);
				//Checked for null values
				if (bcc == null)
				{
				}
				else
				{
					//Added to 'recipients' list
					recipients.add(bcc);	
				}
				
			}
			//To get the 'date'
			else if (line.startsWith("Date: ")) {
				timestamp = stripCommand(line, "Date: ");
				timestamp = "\t" + timestamp;
				
			}
			else if (line.startsWith("\t")) {
				
			}
			if (line.equals("")) { // Empty line indicates the end of the header
				break;
			}
		}
		scanner.close();
		// Replace the following with your code to emit triples
		// (sender, recipient, date) as necessary.
		// Do not forget to check that all components have been properly
		// evaluated (i.e.,, from != null, recipient list is non-empty, and timestamp != null)
		 
		Iterator<String> iter = recipients.iterator();
	String froms[]=null;
		if (from != null) { 
			if(recipients != null)
			{
				if(timestamp != null)
				{
			while(iter.hasNext())
			{
					String v = iter.next();
					//'to' mail address is added as the key
					this.key.set(v);
					//'from' mail addres and timestamp is concatenated and is the value
					v = "\t" + from;
					v = v + "\t" + timestamp; 
					
					this.one.set(v);
					context.write(this.key, this.one);
				}
				}					
			}
		}	
	}

	public void cleanup(Context context) throws IOException,
	InterruptedException {
		// Note: you can use Context methods to emit records in the setup and cleanup as well.

	}
}

static class IndegreeReducer extends Reducer<Text, Text, Text, Text> {
	// You can put instance variables here to store state between iterations of
	// the reduce task.

	//private final Text value = new Text();
	private final Text key1 = new Text();
	private final Text value1 = new Text();
	String s;
	// The setup method. Anything in here will be run exactly once before the
	// beginning of the reduce task.
	public void setup(Context context) throws IOException, InterruptedException {
	}

	// The reducer method
	//key - to address
	//value - from address and timestamp
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {	
		//Hashset to delete the duplicates according to timestamp
		Set<Text> uniques = new HashSet<Text>();
		//List to have all the 'to' addresses
		ArrayList<String> toList = new ArrayList<String>();
		ArrayList<Integer> indeg = new ArrayList<Integer>();
		//To find number of 'to' mail addresses - n value for Indegree
		Set<String> uniqueList = new HashSet<String>();
		//To delete the duplicate values finally
		Set<String> uniqueKey = new HashSet<String>();
		//The uniques set has the unique to-from and timestamp table
		for (Text t : values) 
		{
			if (uniques.contains(t))
		{
		}
		else
		{	uniques.add(t);
			s = t.toString();	
            //The 'to' mail addresses are put into the toList to find the indegree
			toList.add(key.toString());
		}
		}
		int n=0; //Total number of indegree nodes
		uniqueList.addAll(toList);
		for(int i=0;i<uniqueList.size();i++)
		{
			n++;		
		}
		for(String a:toList){
			//The repetition of a particular 'to' address is the indegree value
        	int indegree=Collections.frequency(toList, a); 
        	//indegree is converted to a string
        	String b = ""+ indegree;
        	//The indegree is concatenated with the 'to' address
            String c = a + "\t";
            c = c + b;
            //The repetition of the 'to' mail address with its indegree is removed
            if(uniqueKey.contains(c)){            	
            }
            else{
            	uniqueKey.add(c);
            	String[] st = c.split("\t");
            	key1.set(st[0]);
            	value1.set(st[1]);
            	context.write(key1,value1);
            	
            }
		}
	}
	// The cleanup method. Anything in here will be run exactly once after the
	// end of the reduce task.
	public void cleanup(Context context) throws IOException,InterruptedException {
	}
}
//second mapper class 
//value of this mapper class is the output of the previous reducer class
//value of this mapper class- 'to' mail address with its indegree 
//the indegree is split and given as the key to the mapper
public static class ActualIndegreeMapper extends
Mapper<LongWritable, Text, IntWritable, IntWritable> {
private final static IntWritable one=new IntWritable(1);
private IntWritable key=new IntWritable();
public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
	String str[]=value.toString().split("\t");
	//indegree is given as the key
	this.key.set(Integer.parseInt(str[1]));
	context.write(this.key, one);
}
}
//second reducer class
//key-indegree
//value-number of occurence of the indegree
public static class ActualIndegreeReducer extends
Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
private IntWritable result=new IntWritable();
public void reduce(IntWritable key,Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
int sum=0;
for(IntWritable val:values){
sum+=val.get();
}
result.set(sum);
context.write(key, result);
}
}


public static void printUsage(Tool tool, String extraArgsUsage) {
	System.err.printf("Usage: %s [genericOptions] %s\n\n",
			tool.getClass().getSimpleName(), extraArgsUsage);
	GenericOptionsParser.printGenericCommandUsage(System.err);
}

@Override
public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	if (args.length != 3) {
		printUsage(this, "<input> <output>");
		return 1;
	}
	
	Job job = Job.getInstance(getConf());
	job.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	// Never use FileOutputFormat -- it's abstract!
	// Framework tries to instantiate it, and gets InstantiationException
	//job.setOutputFormatClass(FileOutputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(IndegreeMapper.class);
	job.setReducerClass(IndegreeReducer.class);
//	job.setCombinerClass(MailReaderReducer.class);
	
	job.waitForCompletion(true);
	
	Job job2 = Job.getInstance(getConf());
	job2.setJarByClass(this.getClass());
	FileInputFormat.addInputPath(job2, new Path(args[1]));
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	
	job2.setInputFormatClass(TextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);
	// Never use FileOutputFormat -- it's abstract!
	// Framework tries to instantiate it, and gets InstantiationException
	//job.setOutputFormatClass(FileOutputFormat.class);

	job2.setOutputKeyClass(IntWritable.class);
	job2.setOutputValueClass(IntWritable.class);

	job2.setMapperClass(ActualIndegreeMapper.class);
	job2.setReducerClass(ActualIndegreeReducer.class);
	boolean status = job2.waitForCompletion(true);
	return status ? 0 : 1;
}

public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new ActualIndegree(), args);
	System.exit(exitCode);
			
}}
