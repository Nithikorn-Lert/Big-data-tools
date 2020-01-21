package training;
import java.io.*;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;

// Assume that our txt file contains "lion tiger lion ggwp"
// before run this file you need to change filename to WordCount1_book.java
/* Technical main points
 - The KEY and VALUE classes have to be serialized and need to implement the Writable interface 
 - the KEY classes have to implement the WritableComparable interface to facilitate sorting
 - Main processes of Hadoop can be found here: https://www.skillbook.it/Blog/Details/2170
 
 		 (split)     (map)           (shuffle)			 (reduce)
 	Input ====> K1,V1 ===> List(K2,V2) ====> K2, List(V2) =====> Lits(K3,V3)
 	     <---- SELECT ---->                          <- GROUP BY ->

Ref:
https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Job_Configuration
Learning Hadoop 2 by Garry Turkington and Gabriele Modena
 */


// Hadoop 2.6
public class WordCount1_book extends Configured implements Tool {
	
	  private static final Logger LOG = Logger.getLogger(WordCount1_book.class);

	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new WordCount1_book(), args);
	    System.exit(res);
	  }
	
	public static class wcMapper extends Mapper<Object, Text, Text, IntWritable> {
							   // claas: Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>

		private final static IntWritable one = new IntWritable(1);
		// IntWriable is just a type of wrapper classes of Hadoop.
	 	// Native Java:
			// private final static Integer one = new Integer(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context)  /*	
	   	class:      map(KEYIN key, VALUEIN value, Context context)
        	interface:  map(WritableComparable, Writable, Context) */
		// Context, object to interact with rest of hadoop system.
		   // It unifies the role of JobConf, OutputCollector, and Reporter from old API.
		
			throws IOException, InterruptedException {
				// wordcount function
				String[] words = value.toString().split(" "); // splitting the content of input file
				for (String str: words) {
					word.set(str);
					context.write(word, one);
					// The output of reduce task is typically written to the FileSystem via Context.write(WritableComparable, Writable)
					// In this case, we defined "Context" as "context" at above.

					/* output: List(K2, V2)
					   [ (lion, 1),
						 (tiger, 1),
						 (lion, 1),
						 (ggwp, 1)  ]
					------ Shuffle ---------
				 	   [ (lion, 1),
						 (lion, 1),
						 (tiger, 1),
						 (ggwp, 1)  ] */

				}
			}
	}

	// K2, List(V2) ===(Reduce)===> List(K3, V3)
	public static class wcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
						  		// class: Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) /*
		class:  	reduce(KEYIN key, Iterable<VALUEIN> values,    Context context)						
		interface:  reduce(WritableComparison, Iterable<Writable>, Context)		 */
		// Input of reducer are key and corresponding list of values
			
			throws IOException, InterruptedException {
			int total = 0;
			for (IntWritable val : values) {
				total++ ;
			}
			context.write(key, new IntWritable(total));
			/* output: List(K3, V3)
			[ 	(lion, 2),
				(tiger, 1),
				(ggwp, 1) 	] */
		}
	}
	
	public int run(String[] args) throws Exception {
		// Job is typically used to specify the Mapper, combiner (if any), Partitioner, Reducer, InputFormat, OutputFormat implementations
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(WordCount1_book.class);

		job.setMapperClass(wcMapper.class);
		job.setReducerClass(wcReducer.class);

		// set output class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// I/O format (based on configulaton of job and path directory)
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    return job.waitForCompletion(true) ? 0 : 1;
	}

}
