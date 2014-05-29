package com.time.slot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class ReturnMain {

	private static Logger logger = Logger.getLogger(ReturnMain.class);
	
	public static void main(String[] args) throws IOException{
		List<ReturnType> list = new ArrayList<ReturnType>();
		ReturnType day = new ReturnType();
		day.setName("day");
		day.setTypes(new String[]{"1","2","3","4","5","6","7","14","30"});
		list.add(day);
		
		ReturnType week = new ReturnType();
		week.setName("week");
		week.setTypes(new String[]{"1","2","3","4","5","6","7","8"});
		list.add(week);
		
		ReturnType month = new ReturnType();
		month.setName("month");
		month.setTypes(new String[]{"1","2","3","4","5","6"});
		list.add(month);
		
		
		if(ReturnOne.execute(args)){
			for(ReturnType type : list){
				for(int i=0;i<type.getTypes().length;i++){
					//第一参数是大类，第二个是小类
					if(ReturnTwo.execute(new String[]{type.getName(),type.getTypes()[i]})){
						ReturnThre.execute(new String[]{type.getName(),type.getTypes()[i]});
					}
				}
			}
		}
		logger.info("留存率统计完毕！");
	}
}
