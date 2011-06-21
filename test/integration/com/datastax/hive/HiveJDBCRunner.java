package com.datastax.hive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;

import com.datastax.TestUtils;
import static org.junit.Assert.fail;

public class HiveJDBCRunner {
	
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
        String examplesDir = rootDir + "/resources/hive/examples/files";
    	String testRootDir = rootDir + "/test/integration/com/datastax/hive";
        String dataDir = testRootDir + "/../testData";

    	String testDir = testRootDir + "/testCases/";
    	String resultsDir = testRootDir + "/testResults/";

    	String script = testDir + testScript;
    	String actualOutput = resultsDir + testScript + ".jdbc.out";
    	String expectedOutput = resultsDir + testScript + ".jdbc.exp";


        File outFile = new File(actualOutput);
        if (outFile.exists() == true) 
        {
            outFile.delete();
        }
        
        FileReader fr = new FileReader(new File(script));                      
        BufferedReader br = new BufferedReader(fr);  
        
        FileWriter fstream = new FileWriter(actualOutput);
        BufferedWriter results = new BufferedWriter(fstream);
          
        while((s = br.readLine()) != null)  
        {
            // Ignore empty lines ands comments (starting with "--")
            if(!s.trim().equals("") && !s.startsWith("--")) 
            {
                sb.append(s.trim() + " ");  
            }
        }
        br.close();  

        // Use ";" as a delimiter for each request 
        String[] inst = sb.toString().split(";");  
        
        // Run each SQL Statement
        for(int i = 0; i<inst.length; i++)  
        {
            orig_query = inst[i].trim();
            
            // De-tokenize SQL files
            if(!orig_query.equals("") && !orig_query.startsWith("--")) 
            {
                new_query = orig_query.replace("[[DATA_DIR]]", dataDir);
                new_query = new_query.replace("[[EXAMPLES]]", examplesDir);

                results.write("-- Statement: " + orig_query);
                results.newLine();
                
                //Run Query
                try {
                    res = stmt.executeQuery(new_query);  
                } catch (SQLException e) {
                    results.write(e.toString()); 
                }

                // Not Supported: colCount = res.getMetaData().getColumnCount();
                // Workaround: Iterate thru columns until exception reached.
                while (res.next()) 
                {
                    for (int j=1; j<=colCount; j++) 
                    {
                        try {
                            results.write(res.getString(j) + ", ");    
                        } catch (SQLException e) {
                            if (e.getMessage().startsWith("Invalid columnIndex")) 
                            {
                                break;
                            } else {
                                // Unexpected Failure Parsing Results
                                fail(new_query); 
                                System.out.print(e.toString()); 
                                results.write(e.toString()); 
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
        
        // Diff Results and PASS/FAIL the test case
        TestUtils.diffFiles(actualOutput, expectedOutput);
    }
}
