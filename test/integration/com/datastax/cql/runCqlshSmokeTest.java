package com.datastax.cql;

import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.TestUtils;

public class runCqlshSmokeTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {   
	    String keyspace = "cqldb";

        Connection conn = TestUtils.getJDBCConnection("default");

        String dropKS = "DROP KEYSPACE " + keyspace;        
        JDBCTestRunner.executeCQL("", dropKS, conn);

        String createKS = "CREATE KEYSPACE " + keyspace +
                            " with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' " + 
                            " and strategy_options:replication_factor=1";       
        JDBCTestRunner.executeCQL("", createKS, conn);
        
        conn.close();
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