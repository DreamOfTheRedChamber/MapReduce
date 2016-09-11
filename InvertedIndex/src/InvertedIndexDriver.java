

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InvertedIndexDriver {

	public static void main(String args[]) throws Exception {
		
		if(args.length<2) {
			throw new Exception("Usage: <input directory> <output directory>");
		}
		
		Configuration conf = new Configuration();
		conf.set("filePath", args[2]);
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setNumReduceTasks(3);
		job.setJarByClass(InvertedIndexDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0: 1);
	}
	
}
