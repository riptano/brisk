package com.datastax;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import java.sql.DriverManager;
import java.sql.Connection;
import org.apache.cassandra.cql.jdbc.DriverResolverException;

import static org.junit.Assert.*;

public  class TestUtils {
    
    private static String propFile = System.getProperty("user.dir") + "/test/integration/com/datastax/test.properties";
    
    public static void diffFiles(String actualOutput, String expectedOutput) throws Exception  
    {        
        try {
            BufferedReader abr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(actualOutput))));
            ArrayList<String> actList = new ArrayList<String>(); 

            BufferedReader ebr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(expectedOutput))));
            ArrayList<String> expList = new ArrayList<String>(); 
            
            String line = null;
            
            // Read actual output into array
            while((line = abr.readLine()) != null) {
                actList.add(line);
            }
            
            line = null;
            
            // Read expected output into array
            while((line = ebr.readLine()) != null) {
                expList.add(line);
            }
            
            // Sort both arrays to eliminate sorting failures
            Collections.sort(actList);
            Collections.sort(expList);

            //Compare both arrayas and fail test case if they are not equal
            assertTrue("Diff Found: " + actualOutput, expList.equals(actList));
        } 
        catch (Exception e) {
              e.printStackTrace();
              fail(e.getMessage());
        }        
    }
 
    public static String getLocalHost() throws Exception {
        String hostname = null;
        
        try {
            InetAddress addr = InetAddress.getLocalHost();      

            // Get hostname
            hostname = addr.getHostName();
        } catch (UnknownHostException e) {
        }
        return hostname;
    }
    
    public static String getLocalIP() throws Exception {
        String ip = null;
        
        try {
            InetAddress addr = InetAddress.getLocalHost();
            
            // Get IP Address
            ip = addr.getHostAddress();            
        } catch (UnknownHostException e) {
        }
        return ip;
    }
    
    public static String getCqlshPrompt() throws Exception  
    {  
        String cqlsh = null;
        
        String cServer = null;
        String cServerPort = null;
        String cUser = null;
        String cPassword = null;
        
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(propFile));

            cServer = properties.getProperty("cassandra.server").trim();
            cServerPort = properties.getProperty("cassandra.server.port").trim();
            cUser = properties.getProperty("cassandra.user").trim();
            cPassword = properties.getProperty("cassandra.password".trim());  
            
            if (cUser == null && cPassword == null) {
                cqlsh = "cqlsh " + cServer + " " + cServerPort;  
            } else {
                cqlsh = "cqlsh " + cServer + " " + cServerPort + " -u " + cUser + " -p " + cPassword;  
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
        return cqlsh;
    }
    
    public static Connection getJDBCConnection(String keyspace) throws Exception  
    {  
        String cServer = null;
        String cServerPort = null;
        String cUser = null;
        String cPassword = null;
        
        Connection jdbcConn = null;
        String connectionString = null;
        Boolean retryConnectionWithIP = false;
        Boolean retryConnectionWithHostName = false;

        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(propFile));
            
            cServer = properties.getProperty("cassandra.server").trim();
            cServerPort = properties.getProperty("cassandra.server.port").trim();
            cUser = properties.getProperty("cassandra.user").trim();
            cPassword = properties.getProperty("cassandra.password").trim(); 
            
            connectionString = "jdbc:cassandra:" + cUser +"/" + cPassword + "@" +
            cServer + ":" + cServerPort + "/" + keyspace;
            
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
            jdbcConn = DriverManager.getConnection(connectionString);

        } catch (DriverResolverException ce) 
          {
            // Retry the connection below
              if (cServer.equals("localhost")) {
                  retryConnectionWithIP=true;
              } else {
                  ce.printStackTrace();
                  fail(ce.getMessage()); 
              }
          }       
          catch (Exception e) 
          {
              e.printStackTrace();
              fail(e.getMessage());
          }
          
          // Try substituting the IP Address for localhost and retrying the connection
          if (retryConnectionWithIP == true) {
              try {
                  cServer = getLocalIP();
                  
                  connectionString = "jdbc:cassandra:" + cUser +"/" + cPassword + "@" +
                  cServer + ":" + cServerPort + "/" + keyspace;
                  
                  System.out.println("Retrying Connection String: " + connectionString);            

                  jdbcConn = DriverManager.getConnection(connectionString);
              } catch (DriverResolverException e) {
                  retryConnectionWithHostName = true;
              } 
          }
          
          // Try substituting the hostname for localhost and retrying the connection
          if (retryConnectionWithHostName == true) {
              try {
                  cServer = getLocalHost();

                  connectionString = "jdbc:cassandra:" + cUser +"/" + cPassword + "@" +
                  cServer + ":" + cServerPort + "/" + keyspace;
                  
                  System.out.println("Retrying Connection String: " + connectionString);            

                  jdbcConn = DriverManager.getConnection(connectionString);
              } catch (DriverResolverException e) {
                  e.printStackTrace();
                  fail(e.getMessage());
              } 
          }
    
        return jdbcConn;
    }
    
    public static Connection getHiveConnection() throws Exception
    {  
        Connection hiveConn = null;
        
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(propFile));

            String hiveServer = properties.getProperty("hive.server").trim();
            String hiveServerPort = properties.getProperty("hive.server.port").trim();
            
            if (hiveServer.equals("localhost")) {
                hiveServer = getLocalIP();
            }
            
            String connectionString = "jdbc:hive://" + hiveServer + ":" + hiveServerPort + "/default";
            System.out.println("Connection String: " + connectionString);

            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");

            hiveConn = DriverManager.getConnection(connectionString);
         
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        return hiveConn;
    }
}
