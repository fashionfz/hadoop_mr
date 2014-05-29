package com.time.slot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class ReturnRate implements Writable,DBWritable{

	//prduct+"|"+type
	public String name;
	public int count;
	public void write(PreparedStatement statement) throws SQLException {
		String[] strings = StringUtils.split(name, '|');
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			statement.setDate(1, new java.sql.Date(df.parse(ReturnThre.time).getTime()));
			statement.setString(2, strings[0]);
			statement.setInt(3, Integer.parseInt(strings[1]));
			statement.setInt(4, count);
			statement.setInt(5, ReturnThre.returnType);
			statement.setString(6, ReturnThre.returnStyle);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}
	public void readFields(ResultSet resultSet) throws SQLException {
		this.name = resultSet.getString(2)+"|"+resultSet.getString(3);
		this.count = resultSet.getInt(4);
		
	}
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, name);
		out.writeInt(count);
		
	}
	public void readFields(DataInput in) throws IOException {
		name = Text.readString(in);
		count = in.readInt();
		
	}
	
}
