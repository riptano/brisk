package com.datastax.cql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.InterruptedException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;

import com.datastax.TestUtils;

import static org.junit.Assert.fail;

public class JDBCTestRunner {
	
	//private static final int colCount = 10000;
	private static String rootDir = System.getProperty("user.dir");
	private static String testRootDir = rootDir + "/test/integration/com/datastax/cql";
	private static String testDir = testRootDir + "/testCases/";
	private static String resultsDir = testRootDir + "/testResults/";
	private static String dataDir = testRootDir + "/testData";

	private static String actualOutput = null;
	private static String expectedOutput = null;
	private static String script = null;

	private static BufferedWriter results = null;

	// This method is used to pass a SQL Statement and execute it.  
	// Assumes if you passed a scriptName then you want to write actual output, otherwise it prints to the screen
    public static void executeCQL(String scriptName, String cqlstmt, Connection conn) 
        throws IOException, InterruptedException, SQLException 
    {
        ResultSet res = null;
        PreparedStatement stmt = null;
        
        boolean sqlerr = false;
        int colCount = 10000;        
        
        //Run Query
        try {
            if (!cqlstmt.equals("")) 
            {
                stmt = conn.prepareStatement(cqlstmt);
                res = stmt.executeQuery();
                
                if (cqlstmt.startsWith("CREATE") || cqlstmt.startsWith("DROP")) 
                {
                    Thread.sleep(5000);
                }
                
            } else {
                sqlerr = true;
            }           
        } catch (SQLException e) 
        {
            sqlerr = true;
            // We don't have a "DROP CF/KS ... IF EXISTS", so this acts as a work around.
            // The existing test cases will run CREATE/DROP/USE/CREATE to verify if a drop happened correctly
            if (e.getMessage().startsWith("CF is not defined")) 
            {
                System.out.println("---> IGNORING DROP CF ERROR: " + cqlstmt);
            } 
            else if (e.getMessage().contains("Keyspace does not exist")) 
            {
                System.out.println("---> IGNORING DROP KS ERROR: " + cqlstmt);
            } 
            else if (e.getMessage().startsWith("schema does not match across nodes")) 
            {
                // Pause for 10 seconds when a schema disagreement occurs and retry statement
                System.out.println("---> WARN: Schema Disagreement Occurred. Sleeping 10 Seconds:" + cqlstmt);
                Thread.sleep(10000);
                
                try {
                    // Retry query one more time 
                    res = stmt.executeQuery();
                } catch (SQLException e2) {
                    
                    if (!scriptName.equals("")) {
                        results.write(e2.toString());                  
                        results.newLine(); 
                     } else {
                        System.out.println(e2.toString());       
                    }
                }
            }
            else 
            {
                // There is a real issue here ... 
                //fail("Unexpected SQLException Running Query: " + cqlstmt);

                if (!scriptName.equals("")) {
                    results.write(e.toString());                  
                    results.newLine(); 
                 } else {
                    System.out.println(e.toString());       
                }
            }         
        }    

    // Not Supported: colCount = res.getMetaData().getColumnCount();
    // Workaround: Iterate thru columns until exception reached.
    //int colCount = res.getMetaData().getColumnCount();
        while (sqlerr == false && res.next()) {

            for (int j=1; j<=colCount; j++) {

                try {
                    if (res.getMetaData().getColumnTypeName(j).startsWith("UTF8Type")) {                     
                        if (!scriptName.equals("")) {
                            results.write(res.getString(j) + ", "); 
                        } else {
                            System.out.print(res.getString(j) + ", ");       
                        }                        
                        
                    } else if (res.getMetaData().getColumnTypeName(j).startsWith("Long")) {
                        if (!scriptName.equals("")) {
                            results.write(res.getLong(j) + ", ");
                        } else {
                            System.out.print(res.getLong(j) + ", ");  
                        }
                        
                    } else if (res.getMetaData().getColumnTypeName(j).startsWith("Int")) {
                        if (!scriptName.equals("")) {
                            results.write(res.getInt(j) + ", ");
                        } else {
                            System.out.print(res.getInt(j) + ", ");       
                        }
                        
                    } else if (res.getMetaData().getColumnTypeName(j).startsWith("Object")) {                  
                        if (!scriptName.equals("")) {
                            results.write(res.getObject(j).toString() + ", ");
                        } else {
                            System.out.print(res.getObject(j).toString() + ", ");       
                        }
                        
                    } 
                } 
                catch (IndexOutOfBoundsException e) {
                    // This is encounter when res.get*() is called on an invalid column index
                    break;
                }
                catch (NullPointerException e) {
                    // This is encounter when res.get*() is called on an invalid column index
                    break;
                }
                catch (SQLException e) {                               
                    if (e.getMessage().contains("Invalid column index")) {
                        // This is encountered when getColumnTypeName*() is called on an invalid column index
                        break;
                    } else { 
                        // Unexpected SQLException Parsing Results
                        sqlerr = true;
                        
                        if (!scriptName.equals("")) {
                            // Print all other errors for running negative tests and capturing actual output
                            results.write(e.toString());
                            results.newLine(); 
                            fail("Unexpected SQLException Parsing Results: " + cqlstmt);

                        } else {
                            fail("Unexpected SQLException Parsing Results: " + cqlstmt);
                        }                      
                    }
                }  
            }

            if (!scriptName.equals(null)) {
                results.newLine();
            }
        }
  }

    // Use this method to parse a file, run queries and diff results
    public static void runQueries(String keyspace, String testScript) 
    {
        /* Due to CASSANDRA-2734 we must re-establish the connection after running DDL in order to do operations*/   
        script = testDir + testScript;
        actualOutput = resultsDir + testScript + ".jdbc.out";
        expectedOutput = resultsDir + testScript + ".jdbc.exp";
        
        String s = new String(); 
        String origQuery = new String(); 
        String newQuery = new String();  
        StringBuffer sb = new StringBuffer(); 
        Connection conn = null;
        
        try {
            
            File outFile = new File(actualOutput);
            if (outFile.exists() == true) {
                outFile.delete();
            }
            
            BufferedReader br = new BufferedReader(new FileReader(new File(script)));
            results = new BufferedWriter(new FileWriter(actualOutput));
            
            conn = TestUtils.getJDBCConnection(keyspace);   

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
                // De-tokenize SQL files for file paths
                origQuery = inst[i].trim();
                newQuery = origQuery.replace("[[DATA_DIR]]", dataDir);
                
                if(!origQuery.equals("") && !origQuery.startsWith("--")) 
                { 
                    results.write("-- Statement: " + origQuery);
                    results.newLine();                  
                } 
                
                // Exclude blank lines and comments
                if(!newQuery.equals("") && !newQuery.startsWith("--")) 
                {                    
                    executeCQL(testScript, newQuery, conn);
                } 
            }
        
            // Close DB conn after running test
            conn.close();
            results.close();
            
            // Diff Results and PASS/FAIL the test case
            TestUtils.diffFiles(actualOutput, expectedOutput);
            
        } catch (Exception e) {            
            fail(e.getMessage());
        }
    }
}
