package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.DefaultConnectionConfig;
import org.apache.ftpserver.DefaultDataConnectionConfiguration;
import org.apache.ftpserver.DefaultFtpServerContext;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.interfaces.DataConnectionConfiguration;
import org.apache.ftpserver.interfaces.FtpServerContext;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * Start-up class of FTP server
 */
public class HdfsOverFtpServer {

	static final Logger log = Logger.getLogger(HdfsOverFtpServer.class);

	private static int port = 0;
	private static int sslPort = 0;
	private static String passivePorts = null;
	private static String sslPassivePorts = null;
	private static String hdfsUri = null;
	private static boolean isKerberos = false;
    private static String keytab = null;
    private static String principal = null;
    private static int maxLogins = 0;

	public static void main(String[] args) throws Exception {
		//PropertyConfigurator.configure("log4j.conf");

		loadConfig();

		if (port != 0) {
			startServer();
		}

		if (sslPort != 0) {
			startSSLServer();
		}
	}

	/**
	 * Load configuration
	 *
	 * @throws IOException
	 */
	private static void loadConfig() throws IOException {
		Properties props = new Properties();
		String confPath = System.getProperty("conf.file");
  	//props.load(new FileInputStream(loadResource("hdfs-over-ftp.properties")));
		props.load(new FileInputStream(new File(confPath)));

		try {
			port = Integer.parseInt(props.getProperty("port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("port is not set. so ftp server will not be started");
		}

		try {
			sslPort = Integer.parseInt(props.getProperty("ssl-port"));
			log.info("ssl-port is set. ssl server will be started");
		} catch (Exception e) {
			log.info("ssl-port is not set. so ssl server will not be started");
		}

		if (port != 0) {
			passivePorts = props.getProperty("data-ports");
			if (passivePorts == null) {
				log.error("data-ports is not set");
				System.exit(1);
			}
		}

		if (sslPort != 0) {
			sslPassivePorts = props.getProperty("ssl-data-ports");
			if (sslPassivePorts == null) {
				log.error("ssl-data-ports is not set");
				System.exit(1);
			}
		}

		hdfsUri = props.getProperty("hdfs-uri");
		if (hdfsUri == null) {
			log.error("hdfs-uri is not set");
			System.exit(1);
		}

		String isKerberosParam = props.getProperty("kerberos-enable");
		isKerberos = Boolean.parseBoolean(isKerberosParam);

		if (isKerberos) {
      keytab = props.getProperty("keytab-path");
      principal = props.getProperty("principal");
      if (keytab == null || principal == null || keytab.isEmpty() || principal.isEmpty()) {
        log.error("Kerberos authentication enabled, keytab-path and principal in " +
            "hdfs-over-ftp.properties can not be empty.");
        System.exit(1);
      }

			try {
				maxLogins = Integer.parseInt(props.getProperty("maxLogins"));
			} catch (Exception e) {
				maxLogins = 0;
			}
    }
	}

	/**
	 * Starts FTP server
	 *
	 * @throws Exception
	 */
	public static void startServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp server. port: " + port + " data-ports: " + passivePorts + " hdfs-uri: " + hdfsUri);

		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);

		DefaultFtpServerContext serverContext = new DefaultFtpServerContext();
		DefaultConnectionConfig connectionConfig = new DefaultConnectionConfig();
		connectionConfig.setMaxLogins(maxLogins);
		serverContext.setConnectionConfig(connectionConfig);

		FtpServer server = new FtpServer(serverContext);

		DataConnectionConfiguration dataCon = new DefaultDataConnectionConfiguration();
		dataCon.setPassivePorts(passivePorts);
		server.getListener("default").setDataConnectionConfiguration(dataCon);
		server.getListener("default").setPort(port);

		HdfsUserManager userManager = new HdfsUserManager();

		String usersPath = System.getProperty("users.file");
		final File file = new File(usersPath);

		userManager.setFile(file);

		server.setUserManager(userManager);

		server.setFileSystem(new HdfsFileSystemManager(isKerberos, keytab, principal));

		server.start();
	}

	private static File loadResource(String resourceName) {
		/*final URL resource = HdfsOverFtpServer.class.getResource(resourceName);
		if (resource == null) {
			throw new RuntimeException("Resource not found: " + resourceName);
		}*/
		ClassLoader cL = Thread.currentThread().getContextClassLoader();
		if (cL == null) {
			cL = HdfsOverFtpServer.class.getClassLoader();
		}
		final URL resource = cL.getResource(resourceName);
		if (resource == null) {
			throw new RuntimeException("Resource not found: " + resourceName);
		}

		return new File(resource.getFile());
	}

	/**
	 * Starts SSL FTP server
	 *
	 * @throws Exception
	 */
	public static void startSSLServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp SSL server. ssl-port: " + sslPort + " ssl-data-ports: " + sslPassivePorts + " hdfs-uri: " + hdfsUri);


		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);

		FtpServer server = new FtpServer();

		DataConnectionConfiguration dataCon = new DefaultDataConnectionConfiguration();
		dataCon.setPassivePorts(sslPassivePorts);
		server.getListener("default").setDataConnectionConfiguration(dataCon);
		server.getListener("default").setPort(sslPort);

		MySslConfiguration ssl = new MySslConfiguration();
		ssl.setKeystoreFile(new File("ftp.jks"));
		ssl.setKeystoreType("JKS");
		ssl.setKeyPassword("333333");
		server.getListener("default").setSslConfiguration(ssl);
		server.getListener("default").setImplicitSsl(true);


		HdfsUserManager userManager = new HdfsUserManager();
		userManager.setFile(new File("users.conf"));

		server.setUserManager(userManager);

		server.setFileSystem(new HdfsFileSystemManager(isKerberos, keytab, principal));

		server.start();
	}


}
