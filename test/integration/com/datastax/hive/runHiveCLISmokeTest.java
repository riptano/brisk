package com.datastax.hive;

import org.junit.Ignore;
import org.junit.Test;

public class runHiveCLISmokeTest {
	
    //@Ignore
    @Test
    /* hiveCRUDtable: Create Table, Load Data and Drop */   
    public void hiveCRUDtable() throws Exception {
        HiveCLITestRunner.runQueries("hiveCRUDtable"); 
    } 
    
    //@Ignore
    @Test
    /* hiveDropPartition: Create, Load, Drop and Load Partitioned Table */
    public void hiveDropPartition() throws Exception {
        HiveCLITestRunner.runQueries("hiveDropPartition");
    } 

    //@Ignore
    @Test
    /* hiveCTAS: Create, Load, Drop non-partitioned table */
    public void hiveCTAS() throws Exception {
        HiveCLITestRunner.runQueries("hiveCTAS");
    } 
    
    //@Ignore
    @Test
    /* hiveCreateLike: Create, Load, Drop partitioned table */
    public void hiveCreateLike() throws Exception {
        HiveCLITestRunner.runQueries("hiveCreateLike");
    } 

    //@Ignore
    @Test
    /* hiveAlterTable: Lots of ALTER TABLES and ADD COLUMNS stuff */
    public void hiveAlterTable() throws Exception {
        HiveCLITestRunner.runQueries("hiveAlterTable");     
    } 
    
    //@Ignore
    @Test
    /* hiveMixedCaseTablesNames: LOAD command commented out due to issues with mixed case */
    public void hiveMixedCaseTablesNames() throws Exception {
        HiveCLITestRunner.runQueries("hiveMixedCaseTablesNames");     
    } 
    
    @Ignore
    @Test
    /* hiveCreateIndex: Not Run due DROP INDEX bugs */
    public void hiveCreateIndex() throws Exception {
        //HiveCLITestRunner. runQueries("hiveCreateIndex");     
    }   
}