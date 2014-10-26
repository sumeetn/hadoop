package com.smn.hadoop.spocosy.mr;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FilePathFilter extends Configured implements PathFilter {

	private Pattern pattern;
    private Configuration conf;
    private FileSystem fs;
    
	public boolean accept(Path path) {
		try {
            if (fs.isDirectory(path)) {
                return true;
            } else {
            	String fileName=path.getName();
                Matcher m = pattern.matcher(fileName);
                return m.matches();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
	}
	
	@Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (conf != null) {
            try {
                fs = FileSystem.get(conf);
                pattern = Pattern.compile(conf.get("file.pattern"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

	
	public static void main(String [] args){
		String fileName="21100.xml";
		String p="11.*.xml";
		Pattern pattern = Pattern.compile(p);
		 Matcher m = pattern.matcher(fileName);
		 System.out.println("Is name : " + fileName + " matches "
                 + p + " ? , " + m.matches());
	}
}
