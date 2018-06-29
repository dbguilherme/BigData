package teste;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class WordCountJob  extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
    	int exitCode = ToolRunner.run(new WordCountJob(), args);
    	 System.exit(exitCode);
    }

	@Override
	public int run(String[] arg0) throws Exception {
		 try {
	        	FileUtils.deleteDirectory(new File("/tmp/out4/"));
	        	FileUtils.deleteDirectory(new File("/tmp/out5/"));

	        	Configuration conf = new Configuration();
	          //  conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
	            
	            Job job = Job.getInstance(conf, "WordCountJob");
	            if (job == null) {
	            return -1;
	            }
	         //   job.setNumReduceTasks(8);
	            
	           // job.setJarByClass(WordCountJob.class);
	            job.setMapperClass(WordCountMapper.class);
	            job.setCombinerClass(WordCountReducer.class);
	            job.setReducerClass(WordCountReducer.class);
	            
	            
	      //      job.setInputFormatClass(Text.class);
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(IntWritable.class);
	         //   job.setInputFormatClass(KeyValueTextInputFormat.class);
	            
	            
	            FileInputFormat.addInputPath(job, new Path("/tmp/lixo"));
	            FileOutputFormat.setOutputPath(job, new Path("/tmp/out4/"));
	            job.waitForCompletion(true);
	            ///////////////////////////////////////
	            
	            
	            Configuration conf2 = new Configuration();
	            //conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
	           
	            Job job2 = Job.getInstance(conf2, "WordCountJob2");
	            
	            //job.setJarByClass(WordCountJob.class);
	            job2.setMapperClass(WordCountMapper2.class);
	            job2.setCombinerClass(WordCountReducer2.class);
	            job2.setReducerClass(WordCountReducer2.class);
	            job2.setInputFormatClass(KeyValueTextInputFormat.class);
	            
	      //      job.setInputFormatClass(Text.class);
	            job2.setOutputKeyClass(Text.class);
	            job2.setOutputValueClass(IntWritable.class);
	            
	            job2.setNumReduceTasks(1);
	            FileInputFormat.addInputPath(job2, new Path("/tmp/out4/part-r-00000"));
	            FileOutputFormat.setOutputPath(job2, new Path("/tmp/out5/"));
	            
	            
	            
	           
	           // System.exit(job.waitForCompletion(true) ? 0 : 1);
	           job2.waitForCompletion(true);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		return 0;
	}

}