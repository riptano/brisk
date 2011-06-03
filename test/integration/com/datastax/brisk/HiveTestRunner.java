package com.datastax.brisk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import junitx.framework.FileAssert;

public class HiveTestRunner {
	
	private static final int colCount = 10000;
	private static ResultSet res;

    public static void runQueries(Connection con, String testScript) throws Exception  
    {    	
    	String s = new String(); 
    	String orig_query = new String();  
    	String new_query = new String();  
        StringBuffer sb = new StringBuffer(); 
        
        Statement stmt = con.createStatement();
        
    	String rootDir = System.getProperty("user.dir");
    	String testDir = rootDir + "/test/integration/com/datastax/brisk/testCases/";
    	String resultsDir = rootDir + "/test/integration/com/datastax/brisk/testResults/";
    	String dataDir = rootDir + "/test/integration/com/datastax/brisk/testData";
    	String examplesDir = rootDir + "/resources/hive/examples/files";

    	String script = testDir + testScript;
    	String actualOutput = resultsDir + testScript + ".out";
    	String expectedOutput = resultsDir + testScript + ".exp";
        String[] env = {"BRISK_HOME=" + rootDir};
        File testResultsDir = new File(resultsDir);
    	    	
        try {
            Process cleanOutFileProc = Runtime.getRuntime().exec("rm -f " + actualOutput, env, testResultsDir);

            FileReader fr = new FileReader(new File(script));                      
            BufferedReader br = new BufferedReader(fr);  
            
            FileWriter fstream = new FileWriter(actualOutput);
            BufferedWriter results = new BufferedWriter(fstream);
              
            while((s = br.readLine()) != null)  {
                // Ignore empty lines ands comments (starting with "--")
                if(!s.trim().equals("") && !s.startsWith("--")) 
                {
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
                	try {
                        res = stmt.executeQuery(new_query);  
                	} catch (SQLException e) {
                        results.write(e.toString()); 
                        fail(e.toString());
                	}
                    
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
                					results.write(e.toString()); 
                		            fail(e.toString());
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
		    		    
            // Verify that the two files are identical
            if  ((new File(expectedOutput)).exists()) 
            {
                //FileAssert has cross platform issues - false negatives.
                //FileAssert.assertEquals("-- FILE DIFF FOUND -- \n",new File(expectedOutput), new File(actualOutput));
                
                String diffCmd = "diff -w -b -B -y --suppress-common-lines " + actualOutput + " " + expectedOutput;
                Process proc = Runtime.getRuntime().exec(diffCmd, env, testResultsDir);
                BufferedReader diffbr = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                                
                if (diffbr.read() == -1) 
                {    
                   //Pass the test case since the diff command generated no output
                   assertTrue(true);
                }
                else { 
                    // Sort Files and then Diff
                    String sortDiff = "diff -w -b -B -y --suppress-common-lines <(sort " + actualOutput + ") <(" + expectedOutput + ")";
                    Process sortDiffProc = Runtime.getRuntime().exec(sortDiff, env, testResultsDir);
                    BufferedReader sortDiffBR = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                                    
                    if (sortDiffBR.read() == -1) 
                    {    
                       //Pass the test case since the diff command generated no output
                       fail("===> FAIL: sorting issues");
                    } else {
                        // Print diff output
                        String sortDiffResult = null;     

                        while((sortDiffResult = sortDiffBR.readLine()) != null) {
                            System.out.println(sortDiffResult);
                        }
                        // Fail the test case since a diff was encountered
                        fail("===> FAIL: results diff");  
                    }
                }                
            }
            else {
            	fail("===> FAIL - Expected output file not found: " + expectedOutput);
            }
        } 
        catch (Exception e) {
     		  e.printStackTrace();
              fail(e.getMessage());
        }
    }
}
