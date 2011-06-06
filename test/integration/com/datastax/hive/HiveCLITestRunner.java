package com.datastax.hive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;

import com.datastax.TestUtils;

import static org.junit.Assert.*;

public  class HiveCLITestRunner {

    public static void runQueries(String testScript) throws Exception  
    {    	
    	String s = new String(); 

    	String rootDir = System.getProperty("user.dir");
        String examplesDir = rootDir + "/resources/hive/examples/files";
        String testRootDir = rootDir + "/test/integration/com/datastax/hive";
        String testDir = testRootDir + "/testCases/";
        String resultsDir = testRootDir + "/testResults/";
        String dataDir = testRootDir + "/testData";
        
    	String script = testDir + testScript;
    	String actualOutput = resultsDir + testScript + ".hivecli.out";
    	String expectedOutput = resultsDir + testScript + ".hivecli.exp";
        
        try {
            
            File outFile = new File(actualOutput);
            if (outFile.exists() == true) {
                outFile.delete();
            }

            BufferedReader br = new BufferedReader(new FileReader(new File(script)));  
            BufferedWriter results = new BufferedWriter(new FileWriter(actualOutput));
  
            String tmpScript = script + ".tmp";
            File tmpFile = new File(tmpScript);
            tmpFile.setWritable(true);

            BufferedWriter cwriter = new BufferedWriter(new FileWriter(tmpFile));
            
            cwriter.write("#!/bin/bash \n");
            cwriter.write("bin/brisk hive <<HERE\n");
            
            while((s = br.readLine()) != null)  {
                // Ignore empty lines ands comments (starting with "--")
                if(!s.trim().equals("") && !s.startsWith("--")) 
                {
                    // Replace directory path tokens in test case
                    s = s.replace("[[DATA_DIR]]", dataDir);
                    s = s.replace("[[EXAMPLES]]", examplesDir);   
                    cwriter.write(s.trim() + "\n");  
            	}
            }            
            
           br.close();  

           cwriter.write("quit;\nHERE");
           cwriter.close();
            
           tmpFile.setExecutable(true);
           Process cliProc = Runtime.getRuntime().exec(tmpScript + " 2>&1");
           
           BufferedReader cliBR = new BufferedReader(new InputStreamReader(cliProc.getInputStream()));
           String cliResult = null;     
           
           while((cliResult = cliBR.readLine()) != null) {
               // The actual ouput contains the detokenized path to the data directory which needs to be replaced to make test portable
               cliResult = cliResult.replace(dataDir, "[[DATA_DIR]]");
               cliResult = cliResult.replace(examplesDir, "[[EXAMPLES]]"); 
               
               results.write(cliResult + "\n");
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
