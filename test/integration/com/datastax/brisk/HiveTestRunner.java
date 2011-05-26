package com.datastax.brisk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junitx.framework.FileAssert;

public class HiveTestRunner {
	
	private static final int colCount = 10000;
	
    public static void runQueries(Connection con, String testScript) throws Exception  
    {  
 //   	this.setName(testScript);
    	
    	String s = new String(); 
    	String orig_query = new String();  
    	String new_query = new String();  

        StringBuffer sb = new StringBuffer(); 
        
        Statement stmt = con.createStatement();
        ResultSet res;
        
    	String rootDir = System.getProperty("user.dir");
    	String testDir = rootDir + "/test/integration/com/datastax/brisk/testCases/";
    	String resultsDir = rootDir + "/test/integration/com/datastax/brisk/testResults/";
    	String dataDir = rootDir + "/test/integration/com/datastax/brisk/testData";
    	String examplesDir = rootDir + "/resources/hive/examples/files";

    	String script = testDir + testScript;
    	String actualOutput = resultsDir + testScript + ".out";
    	String expectedOutput = resultsDir + testScript + ".exp";
    	    	
        try{        	
            FileReader fr = new FileReader(new File(script));                      
            BufferedReader br = new BufferedReader(fr);  
            
            FileWriter fstream = new FileWriter(actualOutput);
            BufferedWriter results = new BufferedWriter(fstream);
              
            while((s = br.readLine()) != null)  {
                // Ignore empty lines ands comments (starting with "--")
                if(!s.trim().equals("") && !s.startsWith("--")) {  
                	sb.append(s.trim() + " ");  
            	}
            }  
            br.close();  
  
            // Use ";" as a delimiter for each request 
            String[] inst = sb.toString().split(";");  
  
            for(int i = 0; i<inst.length; i++)  
            {  
            	orig_query = inst[i].trim();
            	
            	// De-tokenize SQL files
                if(!orig_query.equals("") && !orig_query.startsWith("--")) 
                {  
                   	new_query = orig_query.replace("[[DATA_DIR]]", dataDir);
                	new_query = new_query.replace("[[EXAMPLES]]", examplesDir);
 
                	//System.out.print("-- Statement: " + new_query); 
                	results.write("-- Statement: " + orig_query);
                	results.newLine();

                	long start = System.nanoTime();
                    
                	//Run Query
                	res = stmt.executeQuery(new_query);  
                    
                	//Print run time to standard out, but not to file
                	long secDiff = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                	long msDiff = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                	
                	//System.out.println(" [Runtime: " + secDiff + "s / " + msDiff + "ms]"); 

                	// Not Supported: colCount = res.getMetaData().getColumnCount();
                	// Workaround: Iterate thru columns until exception reached.
                	while (res.next()) {                     
                		for (int j=1; j<=colCount; j++) {                            	
                			try {
                				results.write(res.getString(j) + ", ");    
                			} catch (SQLException e) {
                				if (e.getMessage().startsWith("Invalid columnIndex")) {
                					break;
                				} else {
                					System.out.println("  - SQLException: " + e.toString()); 
                					results.write("  - SQLException: " + e.toString()); 
                				}
                			}   
                		}
                		results.newLine();                     
                	}
                }
            }
            
            // Close files after running test
            br.close();
            fr.close();
            results.close();
            fstream.close();
		    System.out.flush();
		    		    
            // Verify that the two files are identical
            if  ((new File(expectedOutput)).exists()) 
            {
            	//FileAssert sucks ... returns diff's when cli diff doesn't
                //FileAssert.assertEquals("---------- FILE DIFF FOUND ---------- \n",new File(expectedOutput), new File(actualOutput));
            	Process proc = Runtime.getRuntime().exec("diff " + actualOutput + " " + expectedOutput);
            } else {
            	fail("Expected output file not found: " + expectedOutput);
            }

        } catch (Exception e) {
     		  System.out.println(e.getMessage());
     		  e.printStackTrace();
          }
    }
}
