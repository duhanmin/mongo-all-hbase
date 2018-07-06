package com.zyxr.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

/**
 * 配置管理组件
 * 
 * @author rttxdu
 *
 */
final public class ConfigurationManager {
	
	private static Properties prop = new Properties();
	
	static {
		try {
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("my.properties"); 
			prop.load(in);  
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
	
	/**
	 * 获取指定key对应的value
	 * @param key 
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
	
	public static  String getSqlTemplate() {
		String result = "";
		ClassLoader classLoader =  ConfigurationManager.class.getClassLoader();
		try {
		    result = IOUtils.toString(classLoader.getResourceAsStream("create-hbase2hive.sql"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	  }
	
	public static void main(String[] args) {
    	System.out.println(getSqlTemplate());
	}
}
