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
import org.apache.log4j.Logger;

/**
 * ע���ļ��͵�¼�ļ��������
 * @author Administrator
 *ע���ļ�������
 *username,email,product
 *denglu 
 *...228|58411ed0f325f6c77f5e61faa106c984,jiengyh23@qq.com,helome,android,20140417091848193,172.16.4.228,null
 *
 *����
 *login,username,email,prduct
 *���
 *username|prduct  1��3
 *

 */
public class ReturnTwo {
	private static Logger logger = Logger.getLogger(ReturnTwo.class);
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {

		//key:username|prduct
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			String[] strings = StringUtils.split(line, ',');
			if (strings[0].equals("login")) {
				String name=strings[1]+"|"+strings[2];
				output.collect(new Text(name), new IntWritable(2));
			} else {//regist
				String username = strings[0];
				//���ע���username������ʹ��email
				if("".equals(username)||"null".equals(username)) {
					username = strings[1];
				}
				String name = username+"|"+strings[2]; 
				output.collect(new Text(name), new IntWritable(1));
			}
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while(values.hasNext()){
				sum = sum + values.next().get();
			}
			//sumΪ1��3��ע���û�
			//sumΪ2��3�ǵ�¼�û�
			//sumΪ3�������û�
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	public static boolean execute(String[] args) throws IOException{
		
		JobConf conf = new JobConf();
		conf.setJobName("RetentionTwo");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		String style = args[0];
		int type = Integer.parseInt(args[1]);
		
    	SimpleDateFormat df = new SimpleDateFormat("yyMMdd");
    	Calendar cal = Calendar.getInstance();
    	cal.add(Calendar.DATE, -1);
    	Path login = new Path("/monitor/user_action/business/"+df.format(cal.getTime())+"/base");
		
		if("day".equals(style)){		
			cal.add(Calendar.DATE, -type);
		}else if("week".equals(style)){
			cal.add(Calendar.WEDNESDAY, -type);
		}else if("month".equals(style)){
			cal.add(Calendar.MONTH, -type);
		}
    	Path regist = new Path("/monitor/user_action/register_user/"+df.format(cal.getTime()));
    	
    	System.out.println("-------->/monitor/user_action/register_user/"+df.format(cal.getTime()));
		FileSystem  system = regist.getFileSystem(conf);

		if(system.exists(login)&&system.exists(regist)){
			
			FileInputFormat.addInputPath(conf,login);
			FileInputFormat.addInputPath(conf, regist);
			
			
			Path out = new Path("/monitor/user_action/business/"+df.format(cal.getTime())+"/"+args[0]+args[1]);
			
			if(system.exists(out)){
				return true;
			}
			FileOutputFormat.setOutputPath(conf, out);
			try{
				RunningJob job = JobClient.runJob(conf);
				return job.isComplete();
			}catch(Exception e){
				logger.info("������ͳ��-2---"+e.getLocalizedMessage());
				return false;
			}
		}
		return true;
	}
}
