package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FileSystemManager;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Impelented FileSystemManager to use HdfsFileSystemView
 */
public class HdfsFileSystemManager implements FileSystemManager {
	private boolean isKerberos;
	private String keytab;
	private String principal;


	public HdfsFileSystemManager(boolean isKerberos, String keytab, String principal) {
		this.isKerberos = isKerberos;
		this.keytab = keytab;
		this.principal = principal;
	}


	public FileSystemView createFileSystemView(final User user) throws FtpException {
		if(isKerberos) {
			if(null == keytab || keytab.isEmpty() || null == principal || principal.isEmpty()) {
				throw new FtpException("Kerberos authentication enabled, keytab-path and principal in " +
						"hdfs-over-ftp.properties can not be empty.");
			} else {
				try {
					UserGroupInformation.loginUserFromKeytab(principal, keytab);
					UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
					ugi.checkTGTAndReloginFromKeytab();
					return UserGroupInformation.createProxyUser(user.getName(),ugi).doAs(
						new PrivilegedExceptionAction<FileSystemView>() {
							@Override
								public FileSystemView run() throws Exception {
								  return new HdfsFileSystemView(user);
								}
							}
					);
				}catch (Exception e) {
					throw new FtpException(e.getMessage());
				}

			}
		} else {
			return new HdfsFileSystemView(user);
		}
	}
}
