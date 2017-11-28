package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FileObject;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class implements all actions to HDFS
 */
public class HdfsFileObject implements FileObject {

  private final Logger log = LoggerFactory.getLogger(HdfsFileObject.class);

  private Path path;
  private HdfsUser user;
  private FileSystem fs;

  /**
   * Constructs HdfsFileObject from path
   *
   * @param path path to represent object
   * @param user accessor of the object
   */
  public HdfsFileObject(String path, User user) throws FtpException {
    this.path = new Path(path);
    this.user = (HdfsUser) user;
    try {
      fs = this.path.getFileSystem(new Configuration());
    } catch (IOException e) {
      log.error("Error when get FileSystem.");
      throw new FtpException(e.getMessage());
    }
  }

  /**
   * Get full name of the object
   *
   * @return full name of the object
   */
  public String getFullName() {
    return path.toString();
  }

  /**
   * Get short name of the object
   *
   * @return short name of the object
   */
  public String getShortName() {
    String full = getFullName();
    int pos = full.lastIndexOf("/");
    if (pos == 0) {
      return "/";
    }
    return full.substring(pos + 1);
  }

  /**
   * HDFS has no hidden objects
   *
   * @return always false
   */
  public boolean isHidden() {
    return false;
  }

  /**
   * Checks if the object is a directory
   *
   * @return true if the object is a directory
   */
  public boolean isDirectory() {
    try {
      log.debug("is directory? : " + path);
      FileStatus fs = this.fs.getFileStatus(path);
      return fs.isDirectory();
    } catch (IOException e) {
      log.debug(path + " is not dir", e);
      return false;
    }
  }

  /**
   * Get HDFS permissions
   *
   * @return HDFS permissions as a FsPermission instance
   * @throws IOException if path doesn't exist so we get permissions of parent object in that case
   */
  private FsPermission getPermissions() throws IOException {
      return this.fs.getFileStatus(path).getPermission();
    }

    /**
     * Checks if the object is a file
     *
     * @return true if the object is a file
     */
  public boolean isFile() {
    try {
      return this.fs.isFile(path);
    } catch (IOException e) {
      log.error(path + " is not file", e);
      return false;
    }
  }

  /**
   * Checks if the object does exist
   *
   * @return true if the object does exist
   */
  public boolean doesExist() {
    try {
      this.fs.getFileStatus(path);
      return true;
    } catch (IOException e) {
      //   log.debug(path + " does not exist", e);
      return false;
    }
  }

  /**
   * Checks if the user has a read permission on the object
   *
   * @return true if the user can read the object
   */
  public boolean hasReadPermission() {
    try {
      FsPermission permissions = getPermissions();
      if (user.getName().equals(getOwnerName())) {
        if (permissions.toString().substring(0, 1).equals("r")) {
          log.debug("PERMISSIONS: " + path + " - " + " read allowed for user");
          return true;
        }
      } else if (user.isGroupMember(getGroupName())) {
        if (permissions.toString().substring(3, 4).equals("r")) {
          log.debug("PERMISSIONS: " + path + " - " + " read allowed for group");
          return true;
        }
      } else {
        if (permissions.toString().substring(6, 7).equals("r")) {
          log.debug("PERMISSIONS: " + path + " - " + " read allowed for others");
          return true;
        }
      }
      log.debug("PERMISSIONS: " + path + " - " + " read denied");
      return false;
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return false;
    }
  }

  private HdfsFileObject getParent() throws FtpException {
    String pathS = path.toString();
    String parentS = "/";
    int pos = pathS.lastIndexOf("/");
    if (pos > 0) {
      parentS = pathS.substring(0, pos);
    }
    return new HdfsFileObject(parentS, user);
  }

  /**
   * Checks if the user has a write permission on the object
   *
   * @return true if the user has write permission on the object
   */
  public boolean hasWritePermission() {
    try {
      FsPermission permissions = getPermissions();
      if (user.getName().equals(getOwnerName())) {
        if (permissions.toString().substring(1, 2).equals("w")) {
          log.debug("PERMISSIONS: " + path + " - " + " write allowed for user");
          return true;
        }
      } else if (user.isGroupMember(getGroupName())) {
        if (permissions.toString().substring(4, 5).equals("w")) {
          log.debug("PERMISSIONS: " + path + " - " + " write allowed for group");
          return true;
        }
      } else {
        if (permissions.toString().substring(7, 8).equals("w")) {
          log.debug("PERMISSIONS: " + path + " - " + " write allowed for others");
          return true;
        }
      }
      log.debug("PERMISSIONS: " + path + " - " + " write denied");
      return false;
    } catch (IOException e) {
      try {
        return getParent().hasWritePermission();
      } catch (FtpException fe) {
        return false;
      }
    }
  }

  /**
   * Checks if the user has a delete permission on the object
   *
   * @return true if the user has delete permission on the object
   */
  public boolean hasDeletePermission() {
    return hasWritePermission();
  }

  /**
   * Get owner of the object
   *
   * @return owner of the object
   */
  public String getOwnerName() {
    try {
      FileStatus fs = this.fs.getFileStatus(path);
      return fs.getOwner();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Get group of the object
   *
   * @return group of the object
   */
  public String getGroupName() {
    try {
      FileStatus fs = this.fs.getFileStatus(path);
      return fs.getGroup();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Get link count
   *
   * @return 3 is for a directory and 1 is for a file
   */
  public int getLinkCount() {
    return isDirectory() ? 3 : 1;
  }

  /**
   * Get last modification date
   *
   * @return last modification date as a long
   */
  public long getLastModified() {
    try {
      FileStatus fs = this.fs.getFileStatus(path);
      return fs.getModificationTime();
    } catch (IOException e) {
      e.printStackTrace();
      return 0;
    }
  }

  /**
   * Get a size of the object
   *
   * @return size of the object in bytes
   */
  public long getSize() {
    try {
      FileStatus fs = this.fs.getFileStatus(path);
      log.info("getSize(): " + path + " : " + fs.getLen());
      return fs.getLen();
    } catch (IOException e) {
      e.printStackTrace();
      return 0;
    }
  }

  /**
   * Create a new dir from the object
   *
   * @return true if dir is created
   */
  public boolean mkdir() {

    if (!hasWritePermission()) {
      log.debug("No write permission : " + path);
      return false;
    }

    try {
      fs.mkdirs(path);
      fs.setOwner(path, user.getName(), user.getMainGroup());
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Delete object from the HDFS filesystem
   *
   * @return true if the object is deleted
   */
  public boolean delete() {
    try {
      fs.delete(path, true);
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Move the object to another location
   *
   * @param fileObject location to move the object
   * @return true if the object is moved successfully
   */
  public boolean move(FileObject fileObject) {
    try {
      fs.rename(path, new Path(fileObject.getFullName()));
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * List files of the directory
   *
   * @return List of files in the directory
   */
  public FileObject[] listFiles() {

    if (!hasReadPermission()) {
      log.debug("No read permission : " + path);
      return null;
    }

    try {
      FileStatus fileStats[] = this.fs.listStatus(path);

      FileObject fileObjects[] = new FileObject[fileStats.length];
      for (int i = 0; i < fileStats.length; i++) {
        fileObjects[i] = new HdfsFileObject(fileStats[i].getPath().toString(), user);
      }
      return fileObjects;
    } catch (IOException | FtpException e) {
      log.error(e.getMessage());
      return null;
    }
  }

  /**
   * Creates output stream to write to the object
   *
   * @param l is not used here
   * @return OutputStream
   * @throws IOException
   */
  public OutputStream createOutputStream(long l) throws IOException {

    // permission check
    if (!hasWritePermission()) {
      throw new IOException("No write permission : " + path);
    }

    try {
      FSDataOutputStream out = fs.create(path);
      fs.setOwner(path, user.getName(), user.getMainGroup());
      return out;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Creates input stream to read from the object
   *
   * @param l is not used here
   * @return OutputStream
   * @throws IOException
   */
  public InputStream createInputStream(long l) throws IOException {
    // permission check
    if (!hasReadPermission()) {
      throw new IOException("No read permission : " + path);
    }
    try {
      FSDataInputStream in = fs.open(path);
      return in;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}