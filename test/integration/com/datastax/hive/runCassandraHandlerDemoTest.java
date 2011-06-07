package com.datastax.hive;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.TestUtils;

public class runCassandraHandlerDemoTest {
	private static Connection connection = null;
	    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	    //Test Database Connection   	
        connection = TestUtils.getHiveConnection();
	    
	    //Generate Demo Data
	    String rootDir = System.getProperty("user.dir");
	    File demoDir = new File(rootDir + "/demos/portfolio_manager");
	    
	    String[] envp = {"BRISK_HOME=" + rootDir};

	    String[] commands = {"ant",
				 "bin/pricer -o INSERT_PRICES",
				 "bin/pricer -o UPDATE_PORTFOLIOS",
				 "bin/pricer -o INSERT_HISTORICAL_PRICES -n 100",
				 rootDir + "/bin/brisk hive -f 10_day_loss.q"
				 };

	    for(int i=0; i<commands.length ;i++){
	    	
    		try {
	    		//System.out.println("Setting up demo: " + commands[i]);	    		
	    		Process proc = Runtime.getRuntime().exec(commands[i], envp, demoDir);
	    		BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	    		String commandResult = null;     
	    		
	    		while((commandResult = br.readLine()) != null) {
	    			System.out.println(commandResult);
	    		}     
	    			             
    		} catch (Exception e) {
    			e.printStackTrace();
                fail(e.getMessage());
		    }
	    }
	}
			
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
		
	@Test
    /* System.out.println("===> cassHandler_Demo: Create External C* Table, Load Data and Drop */   
	public void testCreateLoadDropTable() throws Exception {
		HiveJDBCRunner.runQueries(connection, "cassHandler_Demo"); 
	} 

}
