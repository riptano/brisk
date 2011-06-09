package com.datastax.cql;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class reproDDL_Select_SingleConn {
    
    @Test
    public void simpleSelect() throws Exception {   
        Connection initConn = null;
        Connection connection = null;

        ResultSet res;
        Statement stmt;
        int colCount = 0;
        
        try {
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
            
            // Check create keyspace
            initConn = DriverManager.getConnection("jdbc:cassandra:root/root@127.0.0.1:9160/default");     
            stmt = initConn.createStatement();

            try {
              System.out.println("Running DROP KS Statement");  
              res = stmt.executeQuery("DROP KEYSPACE ks1");  
              res.next();
              
            } catch (SQLException e) {
                if (e.getMessage().startsWith("Keyspace does not exist")) 
                {
                    // Do nothing - this just means you tried to drop something that was not there.
                    // res = stmt.executeQuery("CREATE KEYSPACE ks1 with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  
                } 
            }   
              
            System.out.println("Running CREATE KS Statement");
            res = stmt.executeQuery("CREATE KEYSPACE ks1 with strategy_class =  'org.apache.cassandra.locator.SimpleStrategy' and strategy_options:replication_factor=1");  
            res.next();

            initConn.close();    
            
            // Run Test
            connection = DriverManager.getConnection("jdbc:cassandra:root/root@127.0.0.1:9160/ks1");     
            stmt = connection.createStatement();

            System.out.print("Running CREATE CF Statement");
            res = stmt.executeQuery("CREATE COLUMNFAMILY users (KEY varchar PRIMARY KEY, password varchar, gender varchar, session_token varchar, state varchar, birth_year bigint)");    
            colCount = res.getMetaData().getColumnCount();
            System.out.println(" -- Column Count: " + colCount); 
            res.next();
            connection.close();               

            connection = DriverManager.getConnection("jdbc:cassandra:root/root@127.0.0.1:9160/ks1");     
            stmt = connection.createStatement();
            
            System.out.print("Running INSERT Statement");
            res = stmt.executeQuery("INSERT INTO users (KEY, password) VALUES ('user1', 'ch@nge')");  
            colCount = res.getMetaData().getColumnCount();
            System.out.println(" -- Column Count: " + colCount); 
            res.next();
            
            System.out.print("Running SELECT Statement");
            res = stmt.executeQuery("SELECT KEY, gender, state FROM users");  
            colCount = res.getMetaData().getColumnCount();
            System.out.println(" -- Column Count: " + colCount); 
            res.getRow();
            res.next();
                
            connection.close();               

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
       
}