package com.cs523.problem5;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WeatherApp extends Configured implements Tool {
	
	public static class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text year = new Text();
        private Text temperature = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String yearValue;
        	String temperatureValue;
        	
            for (String token : value.toString().split("\\s+")) {
            	
            	yearValue = token.substring(15, 19);
            	temperatureValue = token.substring(87, 92);
            		
            	year.set(yearValue);
            	temperature.set(temperatureValue);
            	context.write(year, temperature);
            }
        }
		
    }
  
    public static class WeatherReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text year = new Text();
        
        @Override
        public void reduce(Text yearKey, Iterable<Text> temperatureList, Context context) throws IOException, InterruptedException {
        	
        	int totalTemperature = 0;
        	int tempValue = 0;
        	int counter = 0;
        	int average = 0;
        	
        	for (Text temperature : temperatureList) {
        		tempValue = Integer.valueOf(temperature.toString());
        		totalTemperature += tempValue;
        		counter++;
        	}
        		
        	average = totalTemperature / counter;	
        	result.set(average);  
        	
        	year.set(yearKey.toString());
        	
            context.write(year, result);
        }
    }
    
    //Weather Partitioner class
	
    public static class WeatherPartitioner extends Partitioner < Text, Text > {
    	
       @Override
       public int getPartition(Text key, Text value, int numReduceTasks) {
    	   
    	   String year = key.toString();
    	   int yearInt = Integer.valueOf(year);
    	   
          
          if(numReduceTasks == 0) {
        	  
             return 0;
             
          } else if(yearInt < 1930 ) {
        	  
             return 0;
             
          } else {
        	  
             return 1 % numReduceTasks;
          }
          
       }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new WeatherApp(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "WeatherApp");
        job.setJarByClass(WeatherApp.class);

        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);
        job.setPartitionerClass(WeatherPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(2);
        
        File file = new File("./output");
        deleteIfDirectoryExists(file);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private static boolean deleteIfDirectoryExists(File file) {

        if (file.exists() && file.isDirectory()) {

            deleteDirectory(file);

            return true;
        }

        return false;
    }

    private static boolean deleteDirectory(File dir) {

        if (dir.isDirectory()) {
            File[] children = dir.listFiles();

            for (int i = 0; i < children.length; i++) {

                boolean success = deleteDirectory(children[i]);

                if (!success) {
                    return false;
                }
            }
        } // either file or an empty directory
        System.out.println("removing file or directory : " + dir.getName());
        return dir.delete();
    } 

}