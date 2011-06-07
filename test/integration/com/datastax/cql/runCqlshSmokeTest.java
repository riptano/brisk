package com.datastax.cql;

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.TestUtils;

public class runCqlshSmokeTest {
    public static Connection connection = null;
    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {   
	    String keySpace = "cqldb";
	    ResultSet res;

	    try {
	        Connection conn = TestUtils.getJDBCConnection("default");
	        Statement stmt = conn.createStatement();

	        try {
	          res = stmt.executeQuery("DROP KEYSPACE " + keySpace);	   
              res = stmt.executeQuery("CREATE KEYSPACE " + keySpace +
              " with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  

            } catch (SQLException e) {
                if (e.getMessage().startsWith("Keyspace does not exist")) 
                {
                    // Do nothing ... this just means we are dropping a keyspace that didn't exist
                    res = stmt.executeQuery("CREATE KEYSPACE " + keySpace +
                    " with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and " + 
                    " strategy_options:replication_factor=1");  
                } else {
                    fail(e.getMessage());
                }                   
            }   
            conn.close();            

            // Log on to new keyspace
	        connection = DriverManager.getConnection("jdbc:cassandra:root/root@127.0.0.1:9160/" + keySpace);     

	    } catch (Exception e) {
            fail(e.getMessage());
        }
    }
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	@Test
    /* insert_users_batches: Create Table and use Insert/Update/Delete Batches */   
    public void cqlsh_users_CRUD() throws Exception {
		cqlshTestRunner.runQueries("create_users"); 
	    cqlshTestRunner.runQueries("insert_users_batches"); 
        cqlshTestRunner.runQueries("update_delete_users_batches"); 
    }    	  
	
	@Test
	/* create_all_options_table: Create Table, Load Data and Drop */   
	public void cqlsh_all_options_table() throws Exception {
	    cqlshTestRunner.runQueries("create_all_options_table"); 
	}  
	    
    @Test
    /* create_keyspaces_syntax_check: Create Table, Load Data and Drop */   
    public void cqlsh_create_keyspaces_syntax_check() throws Exception {
        cqlshTestRunner.runQueries("create_keyspaces_syntax_check"); 
    }  
       	
	
}