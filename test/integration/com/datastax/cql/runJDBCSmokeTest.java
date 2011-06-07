package com.datastax.cql;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.TestUtils;

public class runJDBCSmokeTest {
    private static String keySpace = "cqldb";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {   
	    
	    try {
	        Connection conn = TestUtils.getJDBCConnection("default");
	        Statement stmt = conn.createStatement();
	        ResultSet res;
	        
	        try {
	          res = stmt.executeQuery("DROP KEYSPACE " + keySpace);	   
              res = stmt.executeQuery("CREATE KEYSPACE " + keySpace +
              " with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  

            } catch (SQLException e) {
                if (e.getMessage().startsWith("Keyspace does not exist")) 
                {
                    res = stmt.executeQuery("CREATE KEYSPACE " + keySpace +
                    " with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  
                }  else {
                    fail(e.getMessage());
                }  
            }             

            conn.close();              

	    } catch (Exception e) {
            fail(e.getMessage());
        }
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
    /* cql_jdbc_users_crud: Create Table, Load Data and Drop */   
    public void cql_jdbc_keyspace_syntax_check() throws Exception {
        JDBCTestRunner.runQueries(keySpace, "create_keyspaces_syntax_check");
    }  
    
}