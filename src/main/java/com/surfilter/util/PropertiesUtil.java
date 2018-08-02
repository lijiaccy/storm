package com.surfilter.util;


import com.surfilter.sy.location.UnicomSpout;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class PropertiesUtil {
	private static final Logger logger =   Logger.getLogger(PropertiesUtil.class);

	public static final String DEFAULT_CONFIG = "config.properties";
	/**
	 * load prop file
	 * @return
	 */
	public static Properties loadProperties() {
		Properties prop = new Properties();
		InputStream in = null;
		try {
			ClassLoader loder = Thread.currentThread().getContextClassLoader();
			in = loder.getResourceAsStream(DEFAULT_CONFIG); // 方式1：配置更新不需要重启JVM
//			 in = loder.getResourceAsStream(propertyFileName); // 方式2：配置更新需重启JVM
			if (in != null) {
				prop.load(in);
			}
		} catch (IOException e) {
			logger.error("load {} error!"+ DEFAULT_CONFIG);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					logger.error("close {} error!"+ DEFAULT_CONFIG);
				}
			}
		}
		return prop;
	}

	/**
	 * load prop value of string
	 * @param key
	 * @return
	 */
	public static String getString(Properties prop, String key) {
		return prop.getProperty(key);
	}

	/**
	 * load prop value of int
	 * @param key
	 * @return
	 */
	public static int getInt(Properties prop, String key) {
		return Integer.parseInt(getString(prop, key));
	}

	/**
	 * load prop value of boolean
	 * @param prop
	 * @param key
     * @return
     */
	public static boolean getBoolean(Properties prop, String key) {
		return Boolean.valueOf(getString(prop, key));
	}

	public static void main(String[] args) {
		Properties prop = loadProperties();
		System.out.println(getString(prop, "mobile.uri"));
	}

}