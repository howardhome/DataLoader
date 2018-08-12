package com.ctoboost.whoop.dataloader;

        import com.datastax.driver.core.*;

        import com.datastax.driver.dse.DseCluster;
        import com.datastax.driver.dse.DseSession;
        import org.apache.commons.cli.*;
        import org.apache.commons.lang.RandomStringUtils;


        import java.sql.Timestamp;


        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.Calendar;
        import java.util.List;
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
    static boolean BATCH = true;
    static String HOSTS = "";

    static int intervalToReportStatus = 1000 * 10; //1 minute
    static int totalCount = 0;

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

        Option hostOption  = OptionBuilder.withArgName( "hosts=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for hosts,  delimited by ," )
                .create( "H" );

        Option batchOption  = OptionBuilder.withArgName( "batch=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for batch mode,  True or False" )
                .create( "B" );

        Options options = new Options();
        // add t option
        options.addOption(rowOption);
        options.addOption(userOption);
        options.addOption(threadOption);
        options.addOption(userStartOption);
        options.addOption(periodOption);
        options.addOption(freqencyOption);
        options.addOption(hostOption);
        options.addOption(batchOption);

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
                THREAD_COUNT = Integer.parseInt(cmd.getOptionProperties("T").getProperty("thread"));
            }

            if (cmd.hasOption("P")){
                PERIOD = Integer.parseInt(cmd.getOptionProperties("P").getProperty("days"));
            }

            if (cmd.hasOption("F")){
                FREQUENCY = Integer.parseInt(cmd.getOptionProperties("F").getProperty("seconds"));
            }

            if (cmd.hasOption("H")){
                HOSTS = cmd.getOptionProperties("H").getProperty("hosts");
            }

            if (cmd.hasOption("B")){
                BATCH = Boolean.parseBoolean(cmd.getOptionProperties("B").getProperty("batch"));
            }
        }
        catch(Exception ex){
            System.out.println("Exception " + ex.getMessage());
        }



        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "DataLoader", options );
        System.out.println("Start data loader as ");
        System.out.println("Rows in a batch: " + ROWS_IN_BATCH);
        System.out.println("How many users: " + USER_COUNT);
        System.out.println("Start from user: " + USER_TO_START);
        System.out.println("Threads used: " + THREAD_COUNT);
        System.out.println("Period used: " + PERIOD + " days");
        System.out.println("Frequency used: " + FREQUENCY + " seconds");
        System.out.println("Hosts used: " + HOSTS );
        System.out.println("Batch mode used: " + BATCH );


        DseCluster cluster = DseCluster.builder().addContactPoints(HOSTS.split(","))
                .withSocketOptions(
                        new SocketOptions()
                                .setReadTimeoutMillis(2000)
                                .setConnectTimeoutMillis(2000)).build();

        DseSession session = cluster.connect("prod");
        //initialize the blocking queue
        BlockingQueue<List<Metrics>> records = new LinkedBlockingQueue<>();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT + 1); // 1 for monitoring thread

        Runnable sendTask = () -> {
            System.out.println("Start sending task " + Thread.currentThread().getId());
            try {
                while(true) {
                    List<Metrics> record = records.take();
                    //Send to DSE
                    sendMetrics(record, session);
                    record = null;

                    recordSentCounts.incrementAndGet();
                }
            } catch (Exception e) {
                System.out.println("Exception in sending data " + e.getMessage());
                e.printStackTrace();
            }
        };

        for(int i = 0; i< THREAD_COUNT; i++){
            executor.submit(sendTask);
        }

        Runnable countingTask = () -> {
            try {
                while(true) {
                    System.out.println("Added : " + totalCount + ", finished " + recordSentCounts.get() + ", remains : " + records.size() + " running : " + THREAD_COUNT);
                    Thread.sleep(intervalToReportStatus);
                }
            }
            catch (Exception ex){

            }
        };
        executor.submit(countingTask);
        //Generate records
        generateMetrics(records);

    }

    private static void sendMetrics(List<Metrics> record, DseSession session) {
        if (BATCH) {
            BatchStatement bs = new BatchStatement();
            record.forEach(metrics -> {
                        String query = "INSERT INTO metrics (user_id, day_part, ts, strap_id, hr, accel_mag, accel, rr, sig_error, hr_confidence, meta)" +
                                " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        Statement s = new SimpleStatement(query, metrics.uid, metrics.day_part, metrics.ts, metrics.strap_id, metrics.hr, metrics.accel_mag, Arrays.asList(metrics.accel), Arrays.asList(metrics.rr), metrics.sig_error, metrics.hr_confidence, metrics.meta);
                        s.setConsistencyLevel(ConsistencyLevel.QUORUM);
                        bs.add(s);
                    }
            );
            session.execute(bs);
        }
        else{
            record.forEach(metrics -> {
                        String query = "INSERT INTO metrics (user_id, day_part, ts, strap_id, hr, accel_mag, accel, rr, sig_error, hr_confidence, meta)" +
                                " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        Statement s = new SimpleStatement(query, metrics.uid, metrics.day_part, metrics.ts, metrics.strap_id, metrics.hr, metrics.accel_mag, Arrays.asList(metrics.accel), Arrays.asList(metrics.rr), metrics.sig_error, metrics.hr_confidence, metrics.meta);
                        s.setConsistencyLevel(ConsistencyLevel.QUORUM);
                        session.execute(s);
                    }
            );
        }

    }

    private static void generateMetrics(BlockingQueue<List<Metrics>> records){


        Calendar calendar = Calendar.getInstance();
        System.out.println(("Started generating data " + calendar.getTime()));
        Float[] floatNumbers = {0.0f, 0.0f, 0.0f};
        String meta = RandomStringUtils.random(512, true, true);
        String strapID = RandomStringUtils.random(10, false, true); //10 digits
        long count = 0;
        //how many rounds need to insert for all uses
        // period divide frequency
        long rounds = (PERIOD * 24 * 60 * 60) / FREQUENCY ;
        System.out.println((rounds + " rounds"));
        long startTime = 0L;
        for(long round = 0; round < rounds; round++) {
            calendar = Calendar.getInstance();
            startTime = calendar.getTime().getTime();
            System.out.println(("Start time " + calendar.getTime() + ":" + startTime));
            for (int i = 0; i < USER_COUNT; i++) {
                //insert batch data for each user in turn
                List<Metrics> data = new ArrayList<>();
                for (int j = 0; j < ROWS_IN_BATCH; j++) {
                    Metrics m = new Metrics();
                    m.uid = USER_TO_START + i;
                    m.day_part = LocalDate.fromMillisSinceEpoch(startTime + j * 1000); //add one second;
                    m.ts = new Timestamp(startTime + j * 1000);
                    m.strap_id = strapID;
                    m.hr = 100;
                    m.accel_mag = 0.0f;
                    m.accel = floatNumbers;
                    m.rr = floatNumbers;
                    m.sig_error = 1;
                    m.hr_confidence = 1;
                    m.meta = meta;
                    data.add(m);

                }
                records.add(data);
                totalCount++;
                try {
                    Thread.sleep(30);
                }
                catch (Exception ex){

                }

            }
            System.out.println(("Finished " + (round+1) + " rounds"));
        }

        System.out.println(("Finished generating data " + calendar.getTime()));
        System.out.println("Generated  " + totalCount + " records");

    }

    private static class Metrics{
        public int uid;
        public LocalDate day_part;
        public Timestamp ts;
        public String strap_id;
        public int hr;
        public float accel_mag;
        Float[] accel;
        Float[] rr;
        public int sig_error;
        public int hr_confidence;
        public String meta;
    }

}
