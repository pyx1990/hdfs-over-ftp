package org.apache.hadoop.contrib.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class to store DFS connection
 */
public class HdfsOverFtpSystem {

	private static FileSystem fs = null;

	public static String HDFS_URI = "";

	//private static String superuser = "error";
	//private static String supergroup = "supergroup";

	private final static Logger log = LoggerFactory.getLogger(HdfsOverFtpSystem.class);


	private static void hdfsInit() {
		Configuration conf = new Configuration();
		Path path = new Path(HDFS_URI);
		//conf.set("hadoop.job.ugi", superuser + "," + supergroup);
		try {
			fs = path.getFileSystem(conf);
		} catch (IOException e) {
			log.error("DFS Initialization error", e);
		}
	}

	public static void setHDFS_URI(String HDFS_URI) {
		HdfsOverFtpSystem.HDFS_URI = HDFS_URI;
	}

	/**
	 * Get dfs
	 *
	 * @return dfs
	 * @throws IOException
	 */
	public static FileSystem getDfs() throws IOException {
		if (fs == null) {
			hdfsInit();
		}
		return fs;
	}
}
