package hdfs;

import java.io.IOException;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSDelete {
	public static void main(String ar[]) throws IOException {
		HDFSDelete obj = new HDFSDelete();
		String file = "ALS-Spark-codeScript_1";
		String targetDirectory = "directory";
		obj.deletefile("directory", false, file);
	}
	public void deletefile(String targetDirectory, boolean isRecursive, String file)throws IOException {
		Configuration conf = new Configuration();
//	    conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    FileSystem  hdfs = FileSystem.get(java.net.URI.create("hdfs://localhost:9000"), conf);
	    String filePath = "/"+targetDirectory+"/"+file;
	    Path path = new Path(filePath);
	    hdfs.delete(path, isRecursive);
//		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//	    conf.addResource(new Path("/usr/local/hadoop/etc/hadoopcore-site.xml"));
//	    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
		
//		Path path=new Path("/directory/ALS-Spark-code");
//		String uri = conf.get("fs.default.name");
//		System.out.println(uri);
//		Path wd = new Path(uri);
//		System.out.println(wd);
//		Path newPath = Path.mergePaths(wd, path);
//		if(hdfs.exists(path)) {
//			hdfs.delete(path, true);
//			System.out.println("yes");
//		}
//		else {
//			System.out.println(path);
//		}
//		//FileStatus file = getFileStatus(path);
//		
//		hdfs.delete(path, true);
//		System.out.println(hdfs);
//		Path workingfile=hdfs.getFileStatus(f);
	    //FileSystem  hdfs = FileSystem.get(URI.create("hdfs://<namenode-hostname>:<port>"), conf);
	    //hdfs.delete(path, false);
	}

}
