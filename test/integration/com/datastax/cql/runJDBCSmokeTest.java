package com.datastax.cql;

import java.sql.Connection;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.TestUtils;

public class runJDBCSmokeTest {
    private static String keySpace = "cqldb";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {   
        Connection conn = TestUtils.getJDBCConnection("default");

	    String dropKS = "DROP KEYSPACE " + keySpace;	    
	    JDBCTestRunner.executeCQL("", dropKS, conn);

	    String createKS = "CREATE KEYSPACE " + keySpace +
                            " with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' " + 
                            " and strategy_options:replication_factor=1";	    
	    JDBCTestRunner.executeCQL("", createKS, conn);
	    
        conn.close();
    }
	   
    
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	@Test
    /* cql_jdbc_users_crud: Create Table, Load Data and Drop */   
    public void cql_jdbc_users_crud() throws Exception {
	    JDBCTestRunner.runQueries(keySpace, "create_users");
		JDBCTestRunner.runQueries(keySpace, "insert_users_sri");
    }    
	
    @Test
    /* cql_jdbc_users_crud: Create Table, Load Data and Drop */   
    public void CQL_jdbc_all_options_table() throws Exception {
        JDBCTestRunner.runQueries(keySpace, "create_all_options_table");
        JDBCTestRunner.runQueries(keySpace, "insert_all_options_table");
    }  
    
    @Test
    /* cql_jdbc_keyspace_syntax_check */   
    public void cql_jdbc_keyspace_syntax_check() throws Exception {
        JDBCTestRunner.runQueries(keySpace, "create_keyspaces_syntax_check");
    }  
    
}