// cc A MapReduce program to count the number of distinct sender Emails in the Enron dataset provided 
// as a collection of Sequence files
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
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

public class MailReader extends Configured implements Tool {

	static class MailReaderMapper extends Mapper<Text, BytesWritable, Text, Text> {

		
		private static Text one = new Text();
		private final Text key = new Text();
		
		
		private String stripCommand(String line, String com) {
			int len = com.length();
			if (line.length() > len)
				return line.substring(len);
			return null;
		}
		
		
		private String procFrom(String line) {
			if (line == null)
				return null;
			String[] froms;
			String from = null;
			do {
				froms = line.split("\\s+|,+", 5);
				// This will only include Email accounts originating from the Enron domain
				if (froms.length == 1 && froms[0].matches(".+@.*[Ee][Nn][Rr][Oo][Nn].+"))
					from = froms[0];
				for (int i = 0; i < froms.length - 1; i++) {
					if (froms[i].matches(".+@.+")) {
						from = froms[i];
						break;
					}
				}
				line = froms[froms.length - 1];
			} while (froms.length > 1 && from == null);
			return from;
		}
		
		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
		}

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
									
			byte[] bytes = value.getBytes();
			Scanner scanner = new Scanner(new ByteArrayInputStream(bytes), "UTF-8");
			String from = null; // Sender email
			String to=null; //getter email
			String cc=null; 
			String bcc=null; 
			ArrayList<String> recipients = new ArrayList<String>(); // Recipient list
			String timestamp = null; // Date
			for (; scanner.hasNext(); ) {
				String line = scanner.nextLine();
				if (line.startsWith("From:")) {
					//To get the 'from' mail address
					from = procFrom(stripCommand(line, "From:"));
				}
				else if (line.startsWith("To:")) {
					//To get the 'to' mail address
					to = procFrom(stripCommand(line,"To:"));
					//Added to 'recipients' list
					recipients.add(to);
				}
				else if (line.startsWith("Cc:")) {
					//To get the 'cc' mail address
					cc = procFrom(stripCommand(line,"Cc:"));
					//Added to 'recipients' list
					recipients.add(cc);
				}
				else if (line.startsWith("Bcc:")) {
					//To get the 'bcc' mail address
					bcc = procFrom(stripCommand(line,"Bcc:"));
					//Added to 'recipients' list
					recipients.add(bcc);
				}
				else if (line.startsWith("Date: ")) {
					//To get the 'date'
				    timestamp = stripCommand(line,"Date:");
				   // System.out.println(timestamp);
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
			//Iterator<String> iter = recipients.iterator();
			int i=0;
			if (from != null) { 
				for(i=0;i<recipients.size();i++){
				    String v = recipients.get(i);
				    v = v + "\t";
				    v = v + timestamp;
				 //The 'from' mail address is the key
				this.key.set(from);
				//The 'to' mail address is the value
				this.one.set(v);
				context.write(this.key, one);
			}		
			}
		}

		public void cleanup(Context context) throws IOException,
		InterruptedException {
			// Note: you can use Context methods to emit records in the setup and cleanup as well.

		}
	}

	static class MailReaderReducer extends Reducer<Text, Text, Text, Text> {
		// You can put instance variables here to store state between iterations of
		// the reduce task.

		private final Text value = new Text();
		
		// The setup method. Anything in here will be run exactly once before the
		// beginning of the reduce task.
		public void setup(Context context) throws IOException, InterruptedException {
		}

		// The reducer method
		//key - from address
	    //value - to address and timestamp
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//int count = 0;
			for (Text iw : values) {
				//count += iw.get();
			//this.value.set(count);
				value.set(iw);
			context.write(key, value);
			}
		}

		// The cleanup method. Anything in here will be run exactly once after the
		// end of the reduce task.
		public void cleanup(Context context) throws IOException,
				InterruptedException {
		}
	}
	
	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n",
				tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
	
	@Override
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException  {
		if (args.length != 2) {
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

		job.setMapperClass(MailReaderMapper.class);
		job.setReducerClass(MailReaderReducer.class);
//		job.setCombinerClass(MailReaderReducer.class);
		
		
		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MailReader(), args);
		System.exit(exitCode);
	}
}