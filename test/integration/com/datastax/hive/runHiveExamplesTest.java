package com.datastax.hive;

import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.TestUtils;

public class runHiveExamplesTest {
    public static Connection connection = null;
    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        connection = TestUtils.getHiveConnection();   
    }
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
	
    //@Ignore
   @Test
   /* movieline_u_data: load and query u_data from movie line demo */
    public void movieline_u_data() throws Exception {
		HiveJDBCRunner.runQueries(connection, "movieline_u_data"); 
    }   
 
   //@Ignore
   @Test
   /* movieline_u_user and movieline_u_occupation: load and query u_user */
    public void movieline_u_user() throws Exception {
		HiveJDBCRunner.runQueries(connection, "movieline_u_occupation"); 
		HiveJDBCRunner.runQueries(connection, "movieline_u_user"); 
    }   

   //@Ignore
   @Test
   /* movieline_u_genre and movieline_u_item: Querying u_items which contains columns for each genre */
    public void movieline_u_item() throws Exception {
		HiveJDBCRunner.runQueries(connection, "movieline_u_genre"); 
		HiveJDBCRunner.runQueries(connection, "movieline_u_item"); 
    }  
   
   @Ignore
   @Test
   /* movieline_u_info: transform key/value input data to load */
    public void movieline_u_info() throws Exception {
		HiveJDBCRunner.runQueries(connection, "movieline_u_info"); 
    }     

   @Ignore
   @Test
   /* apache_weblog: Run Apache Weblog using RegEx Serde */
    public void apache_weblog() throws Exception {
		HiveJDBCRunner.runQueries(connection, "apache_weblog"); 
    } 
}
