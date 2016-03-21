package dp.utility.aerospikeUtility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class InsertRecordsInFile {

	public static void main(String[] args) throws IOException, URISyntaxException {
		String filePath = "hdfs://localhost:9000/directory/input/newfile3.txt";
		String dirpath = args[0];
	     int counter1 = 0;
	     int counter2 = 0;
	     Configuration conf = getConfiguration(filePath);
	     Path path = new Path(filePath);
	     System.out.println(filePath);
	     FileSystem fs = FileSystem.get(new URI(filePath), conf);
	     BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
	     String line = bufferedReader.readLine();
	     FSDataOutputStream fileOut = null;
	     BufferedWriter writer = null;
	     try {
	    	 while(line!=null)
		     {
	    		 Path FileToWrite = new Path(dirpath+"partfile"+counter2+".txt"); 
	    	     fileOut = fs.create(FileToWrite);
	    	     writer = new BufferedWriter(new OutputStreamWriter(fileOut));
		    	 while(counter1 < 10000) {
		    		 String writing = new String(line);
		    		 writer.append(writing);
		    		 writer.newLine();
//		    		 fw = new FileWriter("/home/anoop/Documents/partfiles/partfile"+counter2+".txt", true);
//		    		 fw.write(writer);
//		    		 fw.close();
			         counter1++;
			         line = bufferedReader.readLine();
			         
		    	 }
		    	 writer.flush();
		    	 fileOut.close();
		    	 writer.close();
		    	 counter2++;
		    	 counter1 = 0;
		     } 
	     }catch(Exception e) {
	    	 e.printStackTrace();
	    	 System.out.println("Over");
	     }finally {
	 
		}
	     
	}
	public static Configuration getConfiguration(String filePath){
	    Configuration conf=new Configuration();
	    //conf.set("fs.file.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName()); 
//	    conf.set("fs.s3.impl",org.apache.hadoop.fs.s3.S3FileSystem.class.getName()); 
//	    conf.set("fs.s3n.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    return conf;
	  }

}
