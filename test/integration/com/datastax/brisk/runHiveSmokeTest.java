package com.datastax.brisk;

import java.sql.DriverManager;
import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class runHiveSmokeTest {
    public static Connection connection = null;
    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    }
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
	
	//@Ignore
	@Test
    /* hiveCRUDtable: Create Table, Load Data and Drop */   
    public void hiveCRUDtable() throws Exception {
		HiveTestRunner.runQueries(connection, "hiveCRUDtable"); 
    } 
    
	//@Ignore
	@Test
    /* hiveDropPartition: Create, Load, Drop and Load Partitioned Table */
    public void hiveDropPartition() throws Exception {
		HiveTestRunner.runQueries(connection, "hiveDropPartition");
    } 

    //@Ignore
	@Test
    /* hiveCTAS: Create, Load, Drop non-partitioned table */
    public void hiveCTAS() throws Exception {
		HiveTestRunner.runQueries(connection, "hiveCTAS");
    } 
	
    //@Ignore
	@Test
    /* hiveCreateLike: Create, Load, Drop partitioned table */
    public void hiveCreateLike() throws Exception {
	    HiveTestRunner.runQueries(connection, "hiveCreateLike");
    } 

    //@Ignore
	@Test
    /* hiveAlterTable: Lots of ALTER TABLES and ADD COLUMNS stuff */
    public void hiveAlterTable() throws Exception {
		HiveTestRunner.runQueries(connection, "hiveAlterTable");     
    } 
	
    //@Ignore
	@Test
    /* hiveMixedCaseTablesNames: LOAD command commented out due to issues with mixed case */
    public void hiveMixedCaseTablesNames() throws Exception {
    	HiveTestRunner.runQueries(connection, "hiveMixedCaseTablesNames");     
    } 
    
    @Ignore
	@Test
    /* hiveCreateIndex: Not Run due DROP INDEX bugs */
    public void hiveCreateIndex() throws Exception {
    	//HiveTestRunner. runQueries(connection, "hiveCreateIndex");     
    }  
}