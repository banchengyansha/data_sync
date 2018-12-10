package cn.itcast.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	
	private static final String FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	private static SimpleDateFormat sdf = new SimpleDateFormat(FORMAT_PATTERN);
	
	public static Date parseDate(String datetime) throws ParseException{
		if(datetime != null && !"".equals(datetime)){
			return sdf.parse(datetime);
		}
		return null;
	}
	
	
	public static String formatDate(Date datetime) throws ParseException{
		if(datetime != null ){
			return sdf.format(datetime);
		}
		return null;
	}
	
	public static Long formatStringDateToLong(String datetime) throws ParseException{
		if(datetime != null && !"".equals(datetime)){
			Date d =  sdf.parse(datetime);
			return d.getTime();
		}
		return null;
	}
	
	public static Long formatDateToLong(Date datetime) throws ParseException{
		if(datetime != null){
			return datetime.getTime();
		}
		return null;
	}
}
