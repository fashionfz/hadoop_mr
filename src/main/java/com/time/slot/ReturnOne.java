package com.time.slot;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.sun.istack.logging.Logger;

/**
 * 第一次分析，中间元数据准备
 * 去掉重复的昨日登录用户，格式化文件内容格式与注册一直
 * @author Administrator
 *输出格式：
 *login,username,email,prduct
 */
public class ReturnOne {
	private static Logger logger = Logger.getLogger(ReturnOne.class);
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,NullWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] strings = StringUtils.split(line,'|');
			if(strings.length>2){
				String name = "login,"+strings[2]+","+strings[0];
				output.collect(new Text(name),NullWritable.get());
			}else{
				logger.info("留存率统计---'"+line+"'数据格式不正确！");
			}
			
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,NullWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			output.collect(key,null);
			
		}
		
	}
	
	public static boolean execute(String[] args) throws IOException{
		JobConf conf = new JobConf();
		conf.setJobName("RetentionOne");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
    	SimpleDateFormat df = new SimpleDateFormat("yyMMdd");
    	Calendar cal = Calendar.getInstance();
    	//cal.add(Calendar.DATE, -1);昨天的登录日志放在今天的目录下
    	String in1 = "/monitor/user_action/login_records/"+df.format(cal.getTime())+"/helome/*/";
    	String in2 = "/monitor/user_action/login_records/"+df.format(cal.getTime())+"/hi/*/";
		FileInputFormat.addInputPath(conf, new Path(in1));
		FileInputFormat.addInputPath(conf, new Path(in2));
		Calendar cal2 = Calendar.getInstance();
		cal2.add(Calendar.DATE, -1);
		Path out = new Path("/monitor/user_action/business/"+df.format(cal2.getTime())+"/base");
		FileSystem system = out.getFileSystem(conf);
		if(system.exists(out)){
			return true;
		}
		FileOutputFormat.setOutputPath(conf, out);
		try{
			RunningJob job =JobClient.runJob(conf);
			return job.isComplete();
		}catch(Exception e){
			logger.info("留存率统计-1---"+e.getMessage());
			return false;
		}
	}
	
}
