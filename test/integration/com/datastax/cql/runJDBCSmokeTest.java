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

//import org.apache.cassandra.cql.jdbc.*;

public class runJDBCSmokeTest {
    public static String keySpace = "cqldb";
    public static String connectionString = "jdbc:cassandra:root/root@127.0.0.1:9160/";


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {   
	    ResultSet res;
	    
	    try {
	        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
	        
	        // Check create keyspace
	        Connection conn = DriverManager.getConnection(connectionString + "default");     
	        Statement stmt = conn.createStatement();

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
            connectionString = connectionString + keySpace;     

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
	    JDBCTestRunner.runQueries(connectionString, "create_users");
		JDBCTestRunner.runQueries(connectionString, "insert_users_sri");
    }    
	
    @Test
    /* cql_jdbc_users_crud: Create Table, Load Data and Drop */   
    public void CQL_jdbc_all_options_table() throws Exception {
        JDBCTestRunner.runQueries(connectionString, "create_all_options_table");
        JDBCTestRunner.runQueries(connectionString, "insert_all_options_table");
    }  
    
    @Test
    /* cql_jdbc_users_crud: Create Table, Load Data and Drop */   
    public void cql_jdbc_keyspace_syntax_check() throws Exception {
        JDBCTestRunner.runQueries(connectionString, "create_keyspaces_syntax_check");
    }  
    
}