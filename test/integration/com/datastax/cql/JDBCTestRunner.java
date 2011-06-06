package com.datastax.cql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;

import com.datastax.TestUtils;

import static org.junit.Assert.*;

public class JDBCTestRunner {
	
	//private static final int colCount = 10000;
	private static ResultSet res;

    public static void runQueries(String connectionString, String testScript) throws Exception  
    {   
        /* Due to CASSANDRA-2734 we must re-establish the connection after running DDL in order to do operations*/   
        Connection conn = DriverManager.getConnection(connectionString);   
        PreparedStatement stmt;
        
    	String s = new String(); 
    	String orig_query = new String();  
    	String new_query = new String();  
        StringBuffer sb = new StringBuffer(); 

        String rootDir = System.getProperty("user.dir");
        String testRootDir = rootDir + "/test/integration/com/datastax/cql";
        String testDir = testRootDir + "/testCases/";
        String resultsDir = testRootDir + "/testResults/";
        String dataDir = testRootDir + "/testData";

    	String script = testDir + testScript;
    	String actualOutput = resultsDir + testScript + ".jdbc.out";
    	String expectedOutput = resultsDir + testScript + ".jdbc.exp";
        boolean sqlerr = false;

    	    	
        try {          
            File outFile = new File(actualOutput);
            if (outFile.exists() == true) {
                outFile.delete();
            }
            
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
            	sqlerr = false;
            	
            	// De-tokenize SQL files
                if(!orig_query.equals("") && !orig_query.startsWith("--")) 
                {
                    
                   	new_query = orig_query.replace("[[DATA_DIR]]", dataDir);
                	results.write("-- Statement: " + orig_query);
                	results.newLine();

                    stmt = conn.prepareStatement(new_query);
                    
                	//Run Query
                	try {
                        res = stmt.executeQuery();  
                	} catch (SQLException e) {
                        sqlerr = true;
                        
                        // We don't have a "DROP CF/KS ... IF EXISTS", so this acts as a work around.
                        // The existing test cases will run CREATE/DROP/USE/CREATE to verify if a drop happened correctly
                        if (e.getMessage().startsWith("CF is not defined")) {
                            System.out.println("---> DROP CF ERROR: " + testScript);
                        } else if (e.getMessage().contains("Keyspace does not exist")) {
                            System.out.println("---> DROP KS ERROR: " + testScript);     
                        } else {
                            // Print all other sql errors for running negative tests and capturing actual output
                            results.write(e.toString()); 
                            results.newLine();              
                        }                  
                	}                   
                	
                	// Not Supported: colCount = res.getMetaData().getColumnCount();
                	// Workaround: Iterate thru columns until exception reached.
                    //int colCount = res.getMetaData().getColumnCount();
                    int colCount = 10000;

                	while (res.next() && sqlerr == false) {
                		for (int j=1; j<=colCount; j++) {
                			try {
                			    if (res.getMetaData().getColumnTypeName(j).startsWith("UTF8Type")) {
                                    results.write(res.getString(j) + ", ");            
                			    } else if (res.getMetaData().getColumnTypeName(j).startsWith("Long")) {
                                    results.write(res.getLong(j) + ", ");            
                                } else if (res.getMetaData().getColumnTypeName(j).startsWith("Int")) {
                                    results.write(res.getInt(j) + ", ");            
                                } else if (res.getMetaData().getColumnTypeName(j).startsWith("Object")) {
                                    results.write(res.getObject(j).toString() + ", ");            
                                } 
                			} 
                			catch (IndexOutOfBoundsException e) {
                			    // This is encounter when res.get*() is called on an invalid column index
                			    break;
                			}
                            catch (SQLException e) {                               
                                if (e.getMessage().contains("Invalid column index")) {
                                    // This is encountered when getColumnTypeName*() is called on an invalid column index
                                    break;
                                } else { 
                                    // Unexpected SQLException Parsing Results
                                    System.out.println(e.toString());
                                    results.write(e.toString());
                                    results.newLine();
                                    sqlerr = true;
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
            conn.close();
            
		    // Diff Results and PASS/FAIL the test case
            TestUtils.diffFiles(actualOutput, expectedOutput);
        } 
        catch (Exception e) {
     		  e.printStackTrace();
              fail(e.getMessage());
        }
    }
}
