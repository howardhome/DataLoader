package com.ctoboost.whoop.dataloader;

        import org.apache.commons.cli.*;

        import java.util.Properties;

/**
 * Hello world!
 *
 */

public class App
{
    static int ROWS_IN_BATCH = 600;
    static int USER_COUNT = 10000;
    static int USER_TO_START = 1;
    static int THREAD_COUNT = 10;

    public static void main(String[] args) {



        // create Options object
        Option rowOption  = OptionBuilder.withArgName( "rows=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for how many rows in a batch" )
                .create( "R" );

        Option userOption  = OptionBuilder.withArgName( "users=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for how many users" )
                .create( "U" );

        Option userStartOption  = OptionBuilder.withArgName( "startUser=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for starting range of user like 1000" )
                .create( "S" );

        Option threadOption  = OptionBuilder.withArgName( "thread=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for how many threads used" )
                .create( "T" );

        Options options = new Options();
        // add t option
        options.addOption(rowOption);
        options.addOption(userOption);
        options.addOption(userStartOption);

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("R")){
                ROWS_IN_BATCH = Integer.parseInt(cmd.getOptionProperties("R").getProperty("row"));
            }

            if (cmd.hasOption("U")){
                USER_COUNT = Integer.parseInt(cmd.getOptionProperties("U").getProperty("users"));
            }

            if (cmd.hasOption("S")){
                USER_TO_START = Integer.parseInt(cmd.getOptionProperties("S").getProperty("startUser"));
            }

            if (cmd.hasOption("T")){
                USER_TO_START = Integer.parseInt(cmd.getOptionProperties("T").getProperty("thread"));
            }
        }
        catch(Exception ex){

        }

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "DataLoader", options );
        System.out.println("Start data load as ");
        System.out.println("Ros in a batch: " + ROWS_IN_BATCH);
        System.out.println("How many users: " + USER_COUNT);
        System.out.println("Start from user: " + USER_TO_START);
        System.out.println("Threads used: " + THREAD_COUNT);

    }
}
