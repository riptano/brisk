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
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.Brisk;
import org.apache.cassandra.thrift.Brisk.Iface;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


/**
 * Command line tool for brisk specific commands
 */
public class BriskTool
{
	
    private static final Pair<String, String> HOST_OPT = new Pair<String, String>("h", "host");
    private static final Pair<String, String> PORT_OPT = new Pair<String, String>("p", "port");

    
    private int port = DatabaseDescriptor.getRpcPort();
    private String host = FBUtilities.getLocalAddress().getHostName();
    
    private static ToolOptions options = null;

    static
    {
        options = new ToolOptions();

        options.addOption(HOST_OPT,     true, "node hostname or ip address");
        options.addOption(PORT_OPT,     true, "remote jmx agent port number");
    }
    
    private enum BriskCommand {
        JOBTRACKER,
        MOVEJT
    }
    
    public BriskTool(String[] args) throws Exception
    {
        CommandLineParser parser = new PosixParser();
        ToolCommandLine cmd = null;

        try
        {
            cmd = new ToolCommandLine(parser.parse(options, args));
        }
        catch (ParseException p)
        {
            badUse(p.getMessage());
        }

        String hostParam = cmd.getOptionValue(HOST_OPT.left);
        if (hostParam != null)
        {
        	host = hostParam;
        }
        
        
        String portNum = cmd.getOptionValue(PORT_OPT.left);
        if (portNum != null)
        {
            try
            {
                port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }
        
        BriskCommand command = null;

        try
        {
            command = cmd.getCommand();
        }
        catch (IllegalArgumentException e)
        {
            badUse(e.getMessage());
        }

        // Execute the requested command.
        String[] arguments = cmd.getCommandArguments();
        runCommand(command, arguments);
        
        System.exit(0);
    }


    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        StringBuilder header = new StringBuilder();
        header.append("\nAvailable commands:\n");
        
        // No args
        addCmdHelp(header, "jobtracker", "Returns the jobtracker hostname and port");
        addCmdHelp(header, "movejt", "Move the jobtracker and notifies the Task trakers");
        
        String usage = String.format("java %s [-h|--host=<hostname>] [-p|--port=<#>] <command> <args>%n", BriskTool.class.getSimpleName());
        hf.printHelp(usage, "", options, "");
        System.out.println(header.toString());
    }
    
    private static void addCmdHelp(StringBuilder sb, String cmd, String description)
    {
        sb.append("  ").append(cmd);
        // Ghetto indentation (trying, but not too hard, to not look too bad)
        if (cmd.length() <= 20)
            for (int i = cmd.length(); i < 22; ++i) sb.append(" ");
        sb.append(" - ").append(description).append("\n");
    }
    
    private static void badUse(String useStr)
    {
        System.err.println(useStr);
        printUsage();
        System.exit(1);
    }
    
    /**
     * Run the command entered by the user.
     */
    private void runCommand(BriskCommand cmd, String[] arguments) throws IOException
    {
        Brisk.Iface client = getConnection(); 
        
        switch(cmd)
        {
        case JOBTRACKER:
           getJobTracker(client); 
           break;
        case MOVEJT:
           moveJobTracker(client, arguments[0]); 
           break;
        default:
            throw new IllegalStateException("no handler for command: "+cmd);
        }        
    }
    
    
    private Brisk.Iface getConnection() throws IOException
    {
        TTransport trans = new TFramedTransport(new TSocket(host, port));
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
    
    
    private void moveJobTracker(Iface client, String newJobTracker) {
        try
        {
            String previousJobTracker = client.get_jobtracker_address();
            // TODO (patricioe) move the Job Tracker
            String newJobTrackerReceived = client.move_job_tracker(newJobTracker);
            System.out.println("(Mock operation) JobTracker move from: " + previousJobTracker + " to: " + newJobTrackerReceived);
        }catch(NotFoundException e)
        {
            System.err.println("No jobtracker found");
            System.exit(2);
        }
        catch (TException e)
        {
            System.err.println("Error when moving the Job Tracker: "+e);
            System.exit(2);
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
    
    public static void main(String args[]) throws Exception
    {
        new BriskTool(args);
    }
    
    
    private static class ToolOptions extends Options
    {
        public void addOption(Pair<String, String> opts, boolean hasArgument, String description)
        {
            addOption(opts, hasArgument, description, false);
        }

        public void addOption(Pair<String, String> opts, boolean hasArgument, String description, boolean required)
        {
            addOption(opts.left, opts.right, hasArgument, description, required);
        }

        public void addOption(String opt, String longOpt, boolean hasArgument, String description, boolean required)
        {
            Option option = new Option(opt, longOpt, hasArgument, description);
            option.setRequired(required);
            addOption(option);
        }
    }

    private static class ToolCommandLine
    {
        private final CommandLine commandLine;

        public ToolCommandLine(CommandLine commands)
        {
            commandLine = commands;
        }

        public Option[] getOptions()
        {
            return commandLine.getOptions();
        }

        public boolean hasOption(String opt)
        {
            return commandLine.hasOption(opt);
        }

        public String getOptionValue(String opt)
        {
            return commandLine.getOptionValue(opt);
        }

        public BriskCommand getCommand()
        {
            if (commandLine.getArgs().length == 0)
                throw new IllegalArgumentException("Command was not specified.");

            String command = commandLine.getArgs()[0];

            try
            {
                return BriskCommand.valueOf(command.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unrecognized command: " + command);
            }
        }

        public String[] getCommandArguments()
        {
            List params = commandLine.getArgList();

            if (params.size() < 2) // command parameters are empty
                return new String[0];

            String[] toReturn = new String[params.size() - 1];

            for (int i = 1; i < params.size(); i++)
                toReturn[i - 1] = (String) params.get(i);

            return toReturn;
        }
    }
    
}
