package com.ctoboost.whoop.dataloader;

        import org.apache.commons.cli.*;

        import java.sql.Timestamp;
        import java.util.Date;
        import java.util.List;
        import java.util.Properties;
        import java.util.concurrent.*;
        import java.util.concurrent.atomic.AtomicInteger;

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
    static int PERIOD = 14; //days
    static int FREQUENCY = 60 * 10; // seconds

    static int intervalToReportStatus = 1000 * 60; //1 minute
    static int totalCount;

    public static void main(String[] args) {

        AtomicInteger recordSentCounts = new AtomicInteger(0);

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

        Option periodOption  = OptionBuilder.withArgName( "days=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for data cross how many days" )
                .create( "P" );

        Option freqencyOption  = OptionBuilder.withArgName( "seconds=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for how often to load data, unit is second, default is 600" )
                .create( "F" );

        Options options = new Options();
        // add t option
        options.addOption(rowOption);
        options.addOption(userOption);
        options.addOption(userStartOption);
        options.addOption(periodOption);
        options.addOption(freqencyOption);

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

            if (cmd.hasOption("P")){
                PERIOD = Integer.parseInt(cmd.getOptionProperties("P").getProperty("days"));
            }

            if (cmd.hasOption("F")){
                FREQUENCY = Integer.parseInt(cmd.getOptionProperties("F").getProperty("seconds"));
            }
        }
        catch(Exception ex){

        }

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "DataLoader", options );
        System.out.println("Start data loader as ");
        System.out.println("Ros in a batch: " + ROWS_IN_BATCH);
        System.out.println("How many users: " + USER_COUNT);
        System.out.println("Start from user: " + USER_TO_START);
        System.out.println("Threads used: " + THREAD_COUNT);
        System.out.println("Period used: " + PERIOD + " days");
        System.out.println("Frequency used: " + FREQUENCY + " seconds");

        //initialize the blocking queue
        BlockingQueue<List<Metrics>> records = new LinkedBlockingQueue<>();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        Runnable sendTask = () -> {
            System.out.println("Start sending task " + Thread.currentThread().getId());
            try {
                List<Metrics> record = records.take();
                //Send to DSE
                sendMetrics(record);

                recordSentCounts.incrementAndGet();
            } catch (InterruptedException e) {
                System.out.println("");
                e.printStackTrace();
            }
        };

        for(int i = 0; i< THREAD_COUNT; i++){
            executor.submit(sendTask);
        }

        Runnable countingTask = () -> {
            try {
                Thread.sleep(intervalToReportStatus);
                System.out.println("Total : " + totalCount + ", finished " + recordSentCounts.get() + ", remains : " + records.size());
            }
            catch (Exception ex){

            }
        };
        executor.submit(countingTask);
        //Generate records
        generateMetrics(records);
    }

    private static void sendMetrics(List<Metrics> record){

    }

    private static void generateMetrics(BlockingQueue<List<Metrics>> records){

    }

    private class Metrics{
        public int uid;
        public Date day_part;
        public Timestamp ts;
        public String strap_id;
        public int hr;
        public float accel_mag;
        List<Float> accel_lst;
        List<Float> rr_lst;
        public int sig_error;
        public int hr_confidence;
        public String meta;
    }

}
