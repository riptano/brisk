package com.datastax.bugRepros;

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class reproSchemaDisagreementTest {
    ResultSet res = null;

    @Test
    public void simpleSelect() throws Exception {   
        Connection connection = null;
        Statement stmt = null;
        
        try {
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
            
            // Check create keyspace
            connection = DriverManager.getConnection("jdbc:cassandra:root/root@127.0.0.1:9160/default");     
            stmt = connection.createStatement();

            try {
              System.out.println("Running DROP KS Statement");  
              res = stmt.executeQuery("DROP KEYSPACE ks1");       
              
            } catch (SQLException e) {
                if (e.getMessage().startsWith("Keyspace does not exist")) 
                {
                    // Do nothing - this just means you tried to drop something that was not there.
                    //res = stmt.executeQuery("CREATE KEYSPACE ks1 with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  
                } 
            }   
              
            System.out.println("Running CREATE KS Statement");
            res = stmt.executeQuery("CREATE KEYSPACE ks1 with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  
            connection.close();               
        } 
        catch (SQLException e) {
            if (e.getMessage().startsWith("schema does not match across nodes")) {
                // Pause for 10 seconds
                System.out.println("---> WARN: Schema Disagreement Occurred. Sleeping 10 Seconds.");
                Thread.sleep(10000);
                
                //Retest a connection to the newly created ks
                connection = DriverManager.getConnection("jdbc:cassandra:root/root@127.0.0.1:9160/ks1"); 
                connection.close();               

            } else {
                fail(e.getMessage());
            }
        }
        catch (Exception e) {
            fail(e.getMessage());
        }          
    }
       
}