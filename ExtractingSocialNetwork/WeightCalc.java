import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeightCalc extends Configured implements Tool 
{
	static class WeightCalcMapper extends Mapper<Text, BytesWritable, Text, Text> {

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
			
			String from = null; // Sender email id
			
			ArrayList<String> recipients = new ArrayList<String>(); // Recipients list
			String to = null;
			String cc = null;
			String bcc = null;
			
			String timestamp = null; // Date and Time
			
			for (; scanner.hasNext(); ) 
			{
				String line = scanner.nextLine();
				
				if (line.startsWith("From:")) 
				{//To get the 'from' mail address
					from = procFrom(stripCommand(line, "From: "));
				}
				else if (line.startsWith("To:")) 
				{//To get the 'to' mail address
					to = procFrom(stripCommand(line, "To: "));
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
				else if (line.startsWith("Cc: ")) 
				{//To get the 'cc' mail address
					cc = procFrom(stripCommand(line, "Cc: "));
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
				else if (line.startsWith("Bcc: ")) 
				{//To get the 'bcc' mail address
					bcc = procFrom(stripCommand(line, "Bcc: "));
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
				else if (line.startsWith("Date: ")) 
				{//To get the 'date'
					timestamp = stripCommand(line, "Date: ");
					timestamp = "\t" + timestamp;	
				}
				else if (line.startsWith("\t")) 
				{					
				}
				if (line.equals(""))  // Empty line indicates the end of the header
				{
					break;
				}
			}
			scanner.close();
			
			// To emit (sender, recipient, date).		 
			Iterator<String> iter = recipients.iterator();
		
			if (from != null) 
			{ 
				if(recipients != null)
				{
					if(timestamp != null)
					{	
						//The 'from' mail address is the key
						this.key.set(from);		//From mail id is set as the key.
				
						while(iter.hasNext())
						{
							String v = iter.next();		//in order to get From(tab space)To(tab space)Timestamp
							v = "\t" + v;
							v = v + "\t" + timestamp;  				
							this.one.set(v);
							context.write(this.key, this.one);
						}
					}					
				}
			}	
		}

		public void cleanup(Context context) throws IOException,InterruptedException 
		{
			// Note: you can use Context methods to emit records in the setup and cleanup as well.
		}
	}

	static class WeightCalcReducer extends Reducer<Text, Text, Text, Text> {
		// You can put instance variables here to store state between iterations of the reduce task.

		//private final Text value = new Text();
		
		String s;
		int totcount=0;
		
		private final Text key1 = new Text();
		private final Text value1 = new Text();
		
		// The setup method. Anything in here will be run exactly once before the
		// beginning of the reduce task.
		public void setup(Context context) throws IOException, InterruptedException {
		}

		// The reducer method
		//key - from address
	    //value - to address and timestamp
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			//Hashset to delete the duplicates according to timestamp		
			Set<Text> uniques = new HashSet<Text>();
			
			ArrayList<String> list = new ArrayList<String>();
						
			for (Text t : values) 			//for each value of a particular key
			{	if (uniques.contains(t))	//checking if uniques contain any of the value
				{
				}
				else
				{							//if uniques does not contain any of the value,
					uniques.add(t);			//the value is added to the uniques.
					
					s = t.toString();		//to set only the To mail id to the value. (without timestamp)
					String[] st = s.split("\t");
					s = "\t" + st[1];
					
					String k = key.toString();
					String s1 = k + "\t" + s;
	            	list.add(s1);
				}			
			}
			//To find the total number of arcs
			for(int i=0;i<uniques.size();i++)
			{
				totcount++;		
			}
						
			for(String a:list)
			{
				key1.set(a); 
	        	//To Find the arc between a particular from-to mai address
				int count=Collections.frequency(list, a);  
	        	//To Calculate the weight	        	
	        	double w = ((double)count/totcount);
	        	
	        	String[] str = a.split("\t");
	        	String b = "\t" + w + "(" + str[0] + "," + str[2] + ")";
	        	
	        	value1.set(b);
	        	context.write(key1,value1);
	        }
		}
			
		// The cleanup method. Anything in here will be run exactly once after the
		// end of the reduce task.
		public void cleanup(Context context) throws IOException,InterruptedException 
		{
		}
	}
	
	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n",tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
	
	@Override
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2) 
		{
			printUsage(this, "<input> <output>");
			return 1;
		}
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// Never use FileOutputFormat -- it's abstract!
		// Framework tries to instantiate it, and gets InstantiationException
		//job.setOutputFormatClass(FileOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(WeightCalcMapper.class);
		job.setReducerClass(WeightCalcReducer.class);
//		job.setCombinerClass(MailReaderReducer.class);
		
		
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WeightCalc(), args);
		System.exit(exitCode);
				
	}
}
