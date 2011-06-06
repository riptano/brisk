package com.datastax.hive;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ClassNotFoundException;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.internal.runners.statements.Fail;

public class runCassHandlerCreateObjTest {
	public static Connection connection = null;
    private static final String keySpace      = "fresh_ks";
    private static final String columnFamily  = "fresh_cf_ext";
	    
	@BeforeClass
	public static void setUpBeforeClass() throws 
	InvalidRequestException,TimedOutException, TException, NotFoundException, ClassNotFoundException, SQLException 
    {
        //Test Database Connection
	    try {
	        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
	        connection = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    	
	       } catch (SQLException e) {
               fail("Hive JDBC Login Error: " + e.getMessage());
           }
	      	       
        // Clean up existing Keyspaces and Databases
        try {   
            TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
	    	TProtocol proto = new TBinaryProtocol(tr);
	    	Cassandra.Client client = new Cassandra.Client(proto);
	    	tr.open();
	    	
	        try {   
	            if (DatabaseDescriptor.getTables().contains(columnFamily)) {
	                client.system_drop_column_family(columnFamily);
	            } 
		    	
	            if (client.describe_keyspace(keySpace) != null) {
	                client.system_drop_keyspace(keySpace);
	            } 		    	
	    	} catch (NotFoundException nfe) {
	    	    //Swallow exception - this occurs when keyspace doesn't exist - which is ok
	    	  }
	    		
	    	tr.close();

	    } catch (Exception e) {
	        e.printStackTrace();
            fail(e.getMessage());
	    }
	}
			
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
		
	@Test
    /* System.out.println("===> cassHandler_CreateNewCassObjs: Create New KS and Table in Cassandra */
	public void testCreateLoadDropTable() throws Exception {
		HiveJDBCRunner.runQueries(connection, "cassHandler_CreateNewCassObjs"); 
	} 

}
