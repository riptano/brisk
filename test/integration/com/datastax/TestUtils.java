package com.datastax;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import java.sql.DriverManager;
import java.sql.Connection;

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
   
    public static String getCqlshPrompt() throws Exception  
    {  
        String cqlsh = null;
        
        String cServer = null;
        String cServerPort = null;
        String cUser = null;
        String cPassword = null;
        
        try {
            Properties properties = new Properties();
            cServer = properties.getProperty("cassandra.server");
            cServerPort = properties.getProperty("cassandra.server.port");
            cUser = properties.getProperty("cassandra.user");
            cPassword = properties.getProperty("cassandra.password");  
            
            if (cUser == null && cPassword == null) {
                cqlsh = "cqlsh " + cServer + " " + cServerPort;  
            } else {
                cqlsh = "cqlsh " + cServer + " " + cServerPort + "-u " + cUser + "-p " + cPassword;  
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
        return cqlsh;
    }
    
    public static Connection getJDBCConnection(String keyspace) throws Exception  
    {  
        Connection jdbcConn = null;
        String cServer = null;
        String cServerPort = null;
        String cUser = null;
        String cPassword = null;
        
        try {
            Properties properties = new Properties();

            properties.load(new FileInputStream(propFile));
            
            cServer = properties.getProperty("cassandra.server");
            cServerPort = properties.getProperty("cassandra.server.port");
            cUser = properties.getProperty("cassandra.user");
            cPassword = properties.getProperty("cassandra.password");           
            
            String connectionString = "jdbc:cassandra:" + cUser +"/" + cPassword + "@" +
            cServer + ":" + cServerPort + "/" + keyspace;

            System.out.println("Connection String: " + connectionString);
            
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");

            jdbcConn = DriverManager.getConnection(connectionString);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        return jdbcConn;

    }
    
    public static Connection getHiveConnection() throws Exception
    {  
        Connection hiveConn = null;
        
        try {
            Properties properties = new Properties();

            properties.load(new FileInputStream(propFile));

            String hiveServer = properties.getProperty("hive.server");
            String hiveServerPort = properties.getProperty("hive.server.port");
            
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
