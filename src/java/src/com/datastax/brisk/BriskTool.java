/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.brisk;

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.Brisk;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;


/**
 * Command line tool for brisk specific commands
 */
public class BriskTool
{
    
    private int port = DatabaseDescriptor.getRpcPort();
    private String host = FBUtilities.getLocalAddress().getHostName();
    
    private enum Commands {
        jobtracker
    }
    
    public BriskTool(String[] args)
    {
        if (args.length == 0)
            usage();

        // parse args
        for (int i = 0; i < args.length-1; i++) {
            
            if (args[i].startsWith("--")) {
                int eq = args[i].indexOf("=");

                if (eq < 0)
                    usage();

                String arg = args[i].substring(2, eq);
                String value = args[i].substring(eq + 1);

                try {
                    if (arg.equalsIgnoreCase("host"))
                        host = value;

                    if (arg.equalsIgnoreCase("port"))
                        port = Integer.valueOf(value);
                                                         
                } catch (Throwable t) {
                    usage();
                }
            }
        }
        
        
        
        //process command
        Commands cmd = null;
        try
        {
            cmd = Commands.valueOf(args[args.length -1]);
        }
        catch(Throwable t)
        {
            usage();
        }
        
        try
        {
            runCommand(cmd);
        }
        catch (IOException e)
        {
            System.err.println("Error when running command '" + cmd + "': " + e);
            System.exit(2);
        }
        System.exit(0);
    }
                
    private void usage()
    {
        System.err.print(BriskTool.class.getSimpleName() + " [--host=<hostname>] [--port=<#>] cmd\n"
                + "  Commands:\n"
                + "\tjobtracker     returns the jobtracker hostname and port\n");

        System.exit(1);
    }
    
    private Brisk.Iface getConnection() throws IOException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = new TFramedTransport(socket);
        try
        {
            trans.open();
        }
        catch (TTransportException e)
        {
            throw new IOException("unable to connect to brisk server");
        }

        Brisk.Iface client = new Brisk.Client(new TBinaryProtocol(trans));

        return client;
        
    }
    
    private void runCommand(Commands cmd) throws IOException
    {
        Brisk.Iface client = getConnection(); 
        
        switch(cmd)
        {
        case jobtracker:
           getJobTracker(client); break;
        default:
            throw new IllegalStateException("no handler for command: "+cmd);
        }        
    }
    
    private void getJobTracker(Brisk.Iface client)
    {
        try
        {
            System.out.println(client.get_jobtracker_address());
        }catch(NotFoundException e)
        {
            System.err.println("No jobtracker found");
            System.exit(2);
        }
        catch (TException e)
        {
            System.err.println("Error when fetching jobtracker address: "+e);
            System.exit(2);
        }
    }
    
    public static void main(String args[])
    {
        new BriskTool(args);
    }
    
}
