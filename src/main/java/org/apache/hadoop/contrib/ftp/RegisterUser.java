package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.PasswordEncryptor;

public class RegisterUser {
  public static void main(String[] args) throws Exception{
    PasswordEncryptor passwordEncryptor = new Md5PasswordEncryptor();
    if(args.length == 2){
      String username = args[0];
      String password = args[1];
      String envPassword = passwordEncryptor.encrypt(password);
      StringBuffer userInfo = new StringBuffer();
      userInfo.append("# the " + username + " user");
      userInfo.append("\nftpserver.user." + username + ".userpassword=" + envPassword);
      userInfo.append("\nftpserver.user." + username + ".homedirectory=/");
      userInfo.append("\nftpserver.user." + username + ".enableflag=true");
      userInfo.append("\nftpserver.user." + username + ".writepermission=true");
      userInfo.append("\nftpserver.user." + username + ".maxloginnumber=0");
      userInfo.append("\nftpserver.user." + username + ".maxloginperip=0");
      userInfo.append("\nftpserver.user." + username + ".idletime=0");
      userInfo.append("\nftpserver.user." + username + ".uploadrate=0");
      userInfo.append("\nftpserver.user." + username + ".downloadrate=0");
      userInfo.append("\nftpserver.user." + username + ".groups=hadoop");
      System.out.println(userInfo.toString());
    }else{
      System.out.println("Usage:register-user.sh username password");
    }
  }
}
