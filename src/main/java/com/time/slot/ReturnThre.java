package com.time.slot;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.log4j.Logger;

/*
 * 输入格式
 * username+"|"+prduct   1或3; 
 * 
 *输出
 *prduct|0   10
 *prduct|1    3
 *prduct|3    7
 */
public class ReturnThre {

	private static Logger logger = Logger.getLogger(ReturnThre.class);
	
	public static String time;
	public static int returnType;
	public static String returnStyle;
	
	//value 1或3
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] strings =  line.split("	");  
			String name = strings[0];
			String type = strings[1];
			String prduct = StringUtils.split(name, '|')[1];
			if("3".equals(type)){
				output.collect(new Text(prduct+"|"+type),new IntWritable(1));
				output.collect(new Text(prduct+"|0"),new IntWritable(1));
			}else if("1".equals(type)){
				output.collect(new Text(prduct+"|3"),new IntWritable(0));
				output.collect(new Text(prduct+"|0"),new IntWritable(1));
			}
		}
		
	}
	
	public static class Combin extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum = sum + values.next().get();
			}
			output.collect(key,new IntWritable(sum));
			
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,ReturnRate,IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<ReturnRate, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum = values.next().get();
			}
			ReturnRate rate = new ReturnRate();
			rate.name = key.toString();
			rate.count=sum;
			output.collect(rate,new IntWritable(sum));
			
		}
		
	}
	
	public static boolean execute(String[] args) throws IOException{
		
		JobConf conf = new JobConf();
		conf.setJobName("RetentionTwo");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combin.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);
		
		String style = args[0];
		int type = Integer.parseInt(args[1]);
		
    	SimpleDateFormat df = new SimpleDateFormat("yyMMdd");
    	SimpleDateFormat dfAll = new SimpleDateFormat("yyy-MM-dd");
    	Calendar cal = Calendar.getInstance();
    	cal.add(Calendar.DATE, -1);	
    	
		returnType = type;
		returnStyle = style;
		
    	if("day".equals(style)){
    		cal.add(Calendar.DATE, -type);
    	}else if("week".equals(style)){
    		cal.add(Calendar.WEDNESDAY, -type);
    	}else if("month".equals(style)){
    		cal.add(Calendar.MONTH, -type);
    	}
		

				
		time = dfAll.format(cal.getTime());
		Path in = new Path("/monitor/user_action/business/"+df.format(cal.getTime())+"/"+args[0]+args[1]);
		
		FileSystem system = in.getFileSystem(conf);
		
		if(system.exists(in)){
		
			FileInputFormat.addInputPath(conf, in);
			
	        Properties properties = new Properties();
	        properties.load(TimeSlot.class.getClassLoader().getResourceAsStream("db.properties"));
	        String driver   = (String) properties.get("jdbc.driver");
	        String url      = (String) properties.get("jdbc.url");
	        String username = (String) properties.get("jdbc.username");
	        String password = (String) properties.get("jdbc.password");
	
	        DBConfiguration.configureDB(conf, driver, url, username, password);
	        DBOutputFormat.setOutput(conf, "return_rate","return_date","prduct", "date_type", "user_count","return_type","return_style");
	        try{
		        RunningJob job = JobClient.runJob(conf);
		        return job.isComplete();
	        }catch(Exception e){
	        	logger.info("留存率统计-3---"+e.getMessage());
	        }
		}
		return true;
	}
}
