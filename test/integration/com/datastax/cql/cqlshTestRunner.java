package com.datastax.cql;
import com.datastax.TestUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import static org.junit.Assert.*;


public  class cqlshTestRunner {

    public static void runQueries(String testScript) throws Exception  
    {    	
    	String s = new String(); 

        String rootDir = System.getProperty("user.dir");
        String testRootDir = rootDir + "/test/integration/com/datastax/cql";
        String testDir = testRootDir + "/testCases/";
        String resultsDir = testRootDir + "/testResults/";
        String dataDir = testRootDir + "/testData";
        String examplesDir = rootDir + "/resources/hive/examples/files";

    	String script = testDir + testScript;
    	String actualOutput = resultsDir + testScript + ".cqlsh.out";
    	String expectedOutput = resultsDir + testScript + ".cqlsh.exp";

        try {
            
            File outFile = new File(actualOutput);
            if (outFile.exists() == true) {
                outFile.delete();
            }
            
            BufferedReader br = new BufferedReader(new FileReader(new File(script)));  
            BufferedWriter results = new BufferedWriter(new FileWriter(actualOutput));
  
            String tmpScript = script + ".tmp";
            File tmpFile = new File(tmpScript);
            BufferedWriter cwriter = new BufferedWriter(new FileWriter(tmpFile));
            
            cwriter.write("#!/bin/bash \n");
            cwriter.write(TestUtils.getCqlshPrompt() + " <<HERE\n");
            
            while((s = br.readLine()) != null)  {
                // Ignore empty lines ands comments (starting with "--")
                if(!s.trim().equals("") && !s.startsWith("--")) 
                {
                    s = s.replace("[[DATA_DIR]]", dataDir);
                    s = s.replace("[[EXAMPLES]]", examplesDir);    
                    cwriter.write(s.trim() + "\n");  
            	}
            }            
            
           br.close();  

           cwriter.write("quit;\nHERE");
           cwriter.close();
            
           tmpFile.setExecutable(true);
           Process cqlshProc = Runtime.getRuntime().exec(tmpScript + " 2>&1");
           
           BufferedReader cqlshBR = new BufferedReader(new InputStreamReader(cqlshProc.getInputStream()));
           String csqlshResult = null;     
           
           while((csqlshResult = cqlshBR.readLine()) != null) {
               
               if (csqlshResult.contains("CF is not defined in that keyspace")) {
                   // Do nothing ... since we don't have a "DROP IF ... EXISTS" this acts as a work around
                    System.out.println("Dropping Table that did not exist!"); 
                    System.out.println(csqlshResult); 
                    break;
                }
               
               // The actual ouput contains the detokenized path to the data directory which needs to be replaced to make test portable
               csqlshResult = csqlshResult.replace(dataDir, "[[DATA_DIR]]");
               csqlshResult = csqlshResult.replace(examplesDir, "[[EXAMPLES]]");   
               results.write(csqlshResult + "\n");
           }                                     
           
           // Cleanup files after running test
           tmpFile.delete();
           results.close();
           
           // Diff Results and PASS/FAIL the test case
           TestUtils.diffFiles(actualOutput, expectedOutput);

        } 
        catch (Exception e) {
              e.printStackTrace();
              fail(e.getMessage());
        }
    }
}
