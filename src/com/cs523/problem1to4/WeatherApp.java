package com.cs523;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WeatherApp extends Configured implements Tool {
	
	public static class WeatherMapper extends Mapper<LongWritable, Text, Year, Text> {

		//Problem 1 and 2 : The commented out block of code below 
		//is the map code for problem 1 and 2 
		
		/*private Text year = new Text();
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
        }*/
		
		//private Text year = new Text();
		private Year year = new Year();
        private Text averageTemperature = new Text();
		
		private Map<String, Integer> yearTemperatureMap = new HashMap<String, Integer>();
		private Map<String, Integer> yearCounterMap = new HashMap<String, Integer>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String yearValue;
        	String temperatureValue;
        	
        	int temperature = 0;
        	int totalTemperature = 0;
        	int counter = 0;
        	
            for (String token : value.toString().split("\\s+")) {
            	
            	yearValue = token.substring(15, 19);
            	temperatureValue = token.substring(87, 92);
            	
            	temperature = Integer.valueOf(temperatureValue);
            	
            	if (yearTemperatureMap.get(yearValue) == null) {
            		yearTemperatureMap.put(yearValue, temperature);
            		yearCounterMap.put(yearValue, 1);
            		
            	} else {
            		
            		totalTemperature = yearTemperatureMap.get(yearValue);
            		totalTemperature += temperature;
            		yearTemperatureMap.put(yearValue, totalTemperature);
            		
            		counter = yearCounterMap.get(yearValue);
            		counter = counter + 1;
            		yearCounterMap.put(yearValue, counter);
            		
            	}
            }
        }
        
     // Problem 3, in-map combiner
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	
        	int average = 0;
        	
        	for (String yearKey : yearTemperatureMap.keySet()) {
        		year.setYear(Integer.valueOf(yearKey));
        		average = yearTemperatureMap.get(yearKey) / yearCounterMap.get(yearKey);
        		averageTemperature.set(String.valueOf(average));
        		context.write(year, averageTemperature);
        	}
        }
    }
	
	// The commented out code below is the combiner solution for problem 2 of the Lab
	/*
	 * public static class WeatherCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        
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
        	result.set(String.valueOf(average));        	
            context.write(yearKey, result);
        }
    }*/
  
    public static class WeatherReducer extends Reducer<Year, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text year = new Text();
        
        @Override
        public void reduce(Year yearKey, Iterable<Text> temperatureList, Context context) throws IOException, InterruptedException {
        	
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
        	
        	year.set(String.valueOf(yearKey.getYear()));
        	
            context.write(year, result);
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

        job.setOutputKeyClass(Year.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //job.setNumReduceTasks(2);
        
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
