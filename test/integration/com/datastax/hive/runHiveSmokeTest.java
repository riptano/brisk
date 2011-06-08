package com.datastax.hive;

import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.TestUtils;

public class runHiveSmokeTest {
    public static Connection connection = null;
    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        connection = TestUtils.getHiveConnection();
    }
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
	
	@Test
    /* hiveCRUDtable: Create Table, Load Data and Drop */   
    public void hiveCRUDtable() throws Exception {
		HiveJDBCRunner.runQueries(connection, "hiveCRUDtable"); 
    } 
    
	@Test
    /* hiveDropPartition: Create, Load, Drop and Load Partitioned Table */
    public void hiveDropPartition() throws Exception {
		HiveJDBCRunner.runQueries(connection, "hiveDropPartition");
    } 

	@Test
    /* hiveCTAS: Create, Load, Drop non-partitioned table */
    public void hiveCTAS() throws Exception {
		HiveJDBCRunner.runQueries(connection, "hiveCTAS");
    } 
	
	@Test
    /* hiveCreateLike: Create, Load, Drop partitioned table */
    public void hiveCreateLike() throws Exception {
	    HiveJDBCRunner.runQueries(connection, "hiveCreateLike");
    } 

	@Test
    /* hiveAlterTable: Lots of ALTER TABLES and ADD COLUMNS stuff */
    public void hiveAlterTable() throws Exception {
		HiveJDBCRunner.runQueries(connection, "hiveAlterTable");     
    } 
	
	@Test
    /* hiveMixedCaseTablesNames: LOAD command commented out due to issues with mixed case */
    public void hiveMixedCaseTablesNames() throws Exception {
    	HiveJDBCRunner.runQueries(connection, "hiveMixedCaseTablesNames");     
    } 
    
    @Ignore
	@Test
    /* hiveCreateIndex: Not Run due DROP INDEX bugs */
    public void hiveCreateIndex() throws Exception {
    	//HiveJDBCRunner. runQueries(connection, "hiveCreateIndex");     
    }  
}