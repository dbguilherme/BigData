package hadoop;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 
public class matrixmULTI2 {
 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("A")) {
                outputKey.set(indicesAndValue[2]);
                outputValue.set("A," + indicesAndValue[1] + "," + indicesAndValue[3]);
                context.write(outputKey, outputValue);
            } else {
                outputKey.set(indicesAndValue[1]);
                outputValue.set("B," + indicesAndValue[2] + "," + indicesAndValue[3]);
                context.write(outputKey, outputValue);
            }
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            ArrayList<Entry<Integer, Float>> listA = new ArrayList<Entry<Integer, Float>>();
            ArrayList<Entry<Integer, Float>> listB = new ArrayList<Entry<Integer, Float>>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("A")) {
                    listA.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
                } else {
                    listB.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
                }
            }
            String i;
            float a_ij;
            String k;
            float b_jk;
            Text outputValue = new Text();
            for (Entry<Integer, Float> a : listA) {
                i = Integer.toString(a.getKey());
                a_ij = a.getValue();
                for (Entry<Integer, Float> b : listB) {
                    k = Integer.toString(b.getKey());
                    b_jk = b.getValue();
                    outputValue.set(i + "," + k + "," + Float.toString(a_ij*b_jk));
                    context.write(null, outputValue);
                }
            }
        }
    }
 
    
    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	//System.out.println(key  + " " + value);
        	String temp[]=value.toString().split(",");
        	
        	context.write(new Text(temp[0] + ","+ temp[1]), new Text(temp[2]));   	
        	
        }
    }
        
     public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	float value=0;
            for(Text t: values){
            	System.out.println(t);
            	
            	value+=Float.parseFloat(t.toString());
            	
            }
        	System.out.println(value);
        	
        	context.write(null, new Text(Float.toString(value)));
            }
     }   
    
     public static boolean deleteDir(File dir) {
         if (dir.isDirectory()) {
             String[] children = dir.list();
             for (int i=0; i<children.length; i++) { 
                boolean success = deleteDir(new File(dir, children[i]));
                 if (!success) {
                     return false;
                 }
             }
         }
         return dir.delete();
     }

 
     
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
 
        Job job = new Job(conf, "MatrixMatrixMultiplicationTwoSteps");
        Job job2 = new Job(conf, "MatrixMatrixMultiplicationTwoSteps2");
       
        
        
        deleteDir(new File("out2"));
        deleteDir(new File("out3"));
        
        
        
        
        job.setJarByClass(matrixmULTI2.class);
        job.setInputFormatClass(NLineInputFormat.class);
        //job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
       
 
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("out2"));
        job.waitForCompletion(true);
//        
        job2.setJarByClass(matrixmULTI2.class);
       
        
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        
        job2.setInputFormatClass(NLineInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job2, new Path("out2"));
        FileOutputFormat.setOutputPath(job2, new Path("out3"));
        
        
        job2.waitForCompletion(true);
        
    }
}