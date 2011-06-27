package com.datastax.hive;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.fail;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.TestUtils;
import com.datastax.cql.JDBCTestRunner;

public class runCassHandlerCreateObjTest {
	public static Connection connection = null;
	    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
    {
	    String ks = "fresh_ks";
	    String cf  = "fresh_cf_ext";

        connection = TestUtils.getHiveConnection();
	      	       
        // Use JDBC to clean up existing the KEYSPACE
        try {   
            Connection jdbc_conn = TestUtils.getJDBCConnection(ks);
            String dropCF = "DROP COLUMNFAMILY " + cf;        
            JDBCTestRunner.executeCQL("", dropCF, jdbc_conn);
            jdbc_conn.close();

            jdbc_conn = TestUtils.getJDBCConnection("default");
            String dropKS = "DROP KEYSPACE " + ks;        
            JDBCTestRunner.executeCQL("", dropKS, jdbc_conn);
            jdbc_conn.close();
            
	    } catch (SQLException e) {
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
