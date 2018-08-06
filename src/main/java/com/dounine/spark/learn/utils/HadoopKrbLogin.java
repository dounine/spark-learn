package com.dounine.spark.learn.utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class HadoopKrbLogin {
    private static String KEYTAB = "/etc/security/keytabs/lake.keytab";
    private static String PRINCIPAL = "hbase/lake.dounine.com@dounine.com";

    /**
     * 定期更新进程内的kerberos ticket cache，避免认证过期
     */
    public static class LoginThread implements Runnable {

        long sleepTime = TimeUnit.MINUTES.toMillis(60);

        public LoginThread() {
            Thread thread = new Thread(this, "HadoopKrbLoginThread");
            thread.start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    login();
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Kerberos hadoop 生态认证
     *
     * @return token
     */
    public static void login() {
        try {
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            System.setProperty("sun.security.krb5.debug", "false");
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
