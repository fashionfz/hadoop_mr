package com.time.slot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;

public class LoginArea implements Writable,DBWritable{

	public String name;
	public int visitCount;
	public void write(PreparedStatement statement) throws SQLException {
    	Calendar cal = Calendar.getInstance();
    	cal.add(Calendar.DATE, -1);
    	String[] strings = StringUtils.split(name,'|');
		statement.setString(1, strings[0]);
		statement.setString(2, strings[1]);
		statement.setInt(3, visitCount);
		statement.setDate(4, new java.sql.Date(cal.getTimeInMillis()));
		statement.setInt(5, 0);
		
	}
	public void readFields(ResultSet resultSet) throws SQLException {
		name = resultSet.getString(1)+"|"+resultSet.getString(2);
		visitCount = resultSet.getInt(3);
		
	}
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, name);
		out.writeInt(visitCount);
		
	}
	public void readFields(DataInput in) throws IOException {
		name = Text.readString(in);
		visitCount = in.readInt();
	}
}
