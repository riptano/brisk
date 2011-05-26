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
    public void hiveCRUDtable() throws Exception {
		System.out.println("====> hiveCRUDtable: Create Table, Load Data and Drop");	
		HiveTestRunner.runQueries(connection, "hiveCRUDtable"); 
    } 
    
	//@Ignore
	@Test
    public void hiveDropPartition() throws Exception {
		System.out.println("====> hiveDropPartition: Create, Load, Drop and Load Partitioned Table");
		HiveTestRunner.runQueries(connection, "hiveDropPartition");
    } 

    //@Ignore
	@Test
    public void hiveCTAS() throws Exception {
		System.out.println("====> hiveCTAS: Create, Load, Drop non-partitioned table");
		HiveTestRunner.runQueries(connection, "hiveCTAS");
    } 
	
    //@Ignore
	@Test
    public void hiveCreateLike() throws Exception {
		System.out.println("====> hiveCreateLike: Create, Load, Drop partitioned table");
	    HiveTestRunner.runQueries(connection, "hiveCreateLike");
    } 

    //@Ignore
	@Test
    public void hiveAlterTable() throws Exception {
		System.out.println("====> hiveAlterTable: Lots of ALTER TABLES and ADD COLUMNS stuff");
		HiveTestRunner.runQueries(connection, "hiveAlterTable");     
    } 
	
    //@Ignore
	@Test
    public void hiveMixedCaseTablesNames() throws Exception {
		System.out.println("====> hiveMixedCaseTablesNames: LOAD command commented out due to issues with mixed case");
    	HiveTestRunner.runQueries(connection, "hiveMixedCaseTablesNames");     
    } 
    
    @Ignore
	@Test
    public void hiveCreateIndex() throws Exception {
		System.out.println("====> hiveCreateIndex: Not Run due DROP INDEX bugs");
    	//HiveTestRunner. runQueries(connection, "hiveCreateIndex");     
    }  
}