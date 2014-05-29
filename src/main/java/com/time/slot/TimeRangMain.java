package com.time.slot;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;

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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/**
 * 每天统计昨天的数据
 * 登录时间范围统计
 *
 */
public class TimeRangMain 
{
	
	private static Logger logger = Logger.getLogger(TimeRangMain.class);
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {

		/**
		 * helome|web|429763991@qq.com|2014 03 26 09 45 42 300|0:0:0:0:0:0:0:1|5df3f092b17e949b34f42ca917c5d2a8,429763991@qq.com,helome,web,20140326094542300,0:0:0:0:0:0:0:1,null
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] strings = StringUtils.split(line,'|');
			if(strings.length>3){
				String prduct = strings[0];
				String houre = strings[4].substring(8, 10);
				String rang = prduct+"|"+houre+":00-"+houre+":59";
				output.collect(new Text(rang), new IntWritable(1));
			}else{
				logger.info("登录时段统计---'"+line+"'数据格式不正确！");
			}
		}
		
	}
	
	public static class Combin extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,TimeSlot,IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<TimeSlot, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			TimeSlot obj = new TimeSlot();
			obj.timeRang = key.toString();
			obj.visitCount = sum;
			output.collect(obj, new IntWritable(sum));
		}
	}
	
	
    public static void main( String[] args ) throws IOException
    {
    	JobConf conf = new JobConf();
    	conf.setJobName("monitor_time_slot");
    	
    	conf.setOutputKeyClass(Text.class);
    	conf.setOutputValueClass(IntWritable.class);
    	
    	conf.setMapperClass(Map.class);
    	conf.setCombinerClass(Combin.class);
    	conf.setReducerClass(Reduce.class);
    	
    	conf.setInputFormat(TextInputFormat.class);
    	conf.setOutputFormat(DBOutputFormat.class);
    	SimpleDateFormat df = new SimpleDateFormat("yyMMdd");
    	Calendar cal = Calendar.getInstance();
    	//cal.add(Calendar.DATE, -1);昨天的登录放在今天日期的目录下的
		
    	Path path1 = new Path("/monitor/user_action/login_records/"+df.format(cal.getTime())+"/helome/*/");
    	Path path2 = new Path("/monitor/user_action/login_records/"+df.format(cal.getTime())+"/hi/*/");
    	
    	FileInputFormat.addInputPath(conf, path1);
    	FileInputFormat.addInputPath(conf, path2);
    	
        Properties properties = new Properties();
        properties.load(TimeSlot.class.getClassLoader().getResourceAsStream("db.properties"));
        String driver   = (String) properties.get("jdbc.driver");
        String url      = (String) properties.get("jdbc.url");
        String username = (String) properties.get("jdbc.username");
        String password = (String) properties.get("jdbc.password");

        DBConfiguration.configureDB(conf, driver, url, username, password);
        DBOutputFormat.setOutput(conf, "time_slot","prduct", "time_rang", "visit_count", "visit_date");
        try{
        	JobClient.runJob(conf);
        }catch(Exception e){
        	logger.info("登录时段统计---"+e.getMessage());
        }
        logger.info("登录时段统计完毕！");
    	System.exit(-1);
    }
}
