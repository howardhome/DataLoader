package com.ctoboost.whoop.dataloader;

        import com.datastax.driver.core.*;

        import com.datastax.driver.dse.DseCluster;
        import com.datastax.driver.dse.DseSession;
        import org.apache.commons.cli.*;
        import org.apache.commons.lang.RandomStringUtils;


        import java.sql.Timestamp;


        import java.text.SimpleDateFormat;
        import java.util.*;
        import java.util.concurrent.*;
        import java.util.concurrent.atomic.AtomicInteger;


public class App
{
    static int ROWS_IN_BATCH = 600;
    static int USER_COUNT = 10000;
    static int USER_TO_START = 1;
    static int THREAD_COUNT = 10;
    static float PERIOD = 14; //days
    static int FREQUENCY = 60 * 10; // seconds
    static int TIMERANGE = 4; // 4 hours
    static int WAIT = 1;
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

        Option timeRangeOption  = OptionBuilder.withArgName( "hours=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for time range of partition key in hours " )
                .create( "L" );

        Option waitOption  = OptionBuilder.withArgName( "wait=value" )
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for how long to wait in a batch, 1 means  wait 10 minutes, 2 is 5 minutes, 4 is 2.5 minutes " )
                .create( "W" );

        Options options = new Options();
        // add t option
        options.addOption(rowOption);
        options.addOption(userOption);
        options.addOption(threadOption);
        options.addOption(userStartOption);
        options.addOption(periodOption);
        options.addOption(freqencyOption);
        options.addOption(hostOption);
        options.addOption(timeRangeOption);
        options.addOption(batchOption);
        options.addOption(waitOption);

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
                PERIOD = Float.parseFloat(cmd.getOptionProperties("P").getProperty("days"));
            }

            if (cmd.hasOption("F")){
                FREQUENCY = Integer.parseInt(cmd.getOptionProperties("F").getProperty("seconds"));
            }

            if (cmd.hasOption("H")){
                HOSTS = cmd.getOptionProperties("H").getProperty("hosts");
            }

            if (cmd.hasOption("L")){
                TIMERANGE = Integer.parseInt(cmd.getOptionProperties("L").getProperty("hours"));
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
        System.out.println("Time range used: " + TIMERANGE );

        DseCluster cluster = DseCluster.builder().addContactPoints(HOSTS.split(","))
                .withSocketOptions(
                        new SocketOptions()
                                .setReadTimeoutMillis(2000)
                                .setConnectTimeoutMillis(2000)).build();

        DseSession session = cluster.connect("whoop");
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
                        String query = "INSERT INTO metrics (user_id, day_part, ts, accel_mag, accel_x, accel_y, accel_z, hr, hr_confidence, rr_0, rr_1, rr_2, rr_3, rr_4, sig_error, strap_id )" +
                                " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)";
                        Statement s = new SimpleStatement(query, metrics.uid, metrics.day_part, metrics.ts,  metrics.accel_mag, metrics.accel_x,metrics.accel_y,metrics.accel_z, metrics.hr, metrics.hr_confidence,
                metrics.rr_0,metrics.rr_1,metrics.rr_2,metrics.rr_3,metrics.rr_4, metrics.sig_error,  metrics.strap_id);
                        s.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
                        bs.add(s);
                    }
            );
            session.execute(bs);
        }
        else{
            record.forEach(metrics -> {
                String query = "INSERT INTO metrics (user_id, day_part, ts, accel_mag, accel_x, accel_y, accel_z, hr, hr_confidence, rr_0, rr_1, rr_2, rr_3, rr_4, sig_error, strap_id )" +
                        " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)";
                Statement s = new SimpleStatement(query, metrics.uid, metrics.day_part, metrics.ts,  metrics.accel_mag, metrics.accel_x,metrics.accel_y,metrics.accel_z, metrics.hr, metrics.hr_confidence,
                        metrics.rr_0,metrics.rr_1,metrics.rr_2,metrics.rr_3,metrics.rr_4, metrics.sig_error,  metrics.strap_id);
                        s.setConsistencyLevel(ConsistencyLevel.QUORUM);
                        session.execute(s);
                    }
            );
        }

    }

    private static void generateMetrics(BlockingQueue<List<Metrics>> records){


        Calendar calendar = Calendar.getInstance();
        System.out.println(("Started generating data " + calendar.getTime()));
        Float floatNumbers = 0.0f;
        String meta = RandomStringUtils.random(512, true, true);
        String strapID = RandomStringUtils.random(10, false, true); //10 digits
        long count = 0;
        //how many rounds need to insert for all uses
        // period divide frequency
        long rounds = (long)((PERIOD * 24 * 60 * 60) / FREQUENCY );
        System.out.println((rounds + " rounds"));
        long startTime = 0L;
        calendar = Calendar.getInstance();
        startTime = calendar.getTime().getTime();

        for(long round = 0; round < rounds; round++) {
            long start = System.currentTimeMillis();
            long beginTime = startTime + round * FREQUENCY * 1000;
            int sleepCount = 0;
            for (int i = 0; i < USER_COUNT; i++) {
                //insert batch data for each user in turn
                List<Metrics> data = new ArrayList<>();

                for (int j = 0; j < ROWS_IN_BATCH; j++) {
                    long interval = beginTime + j * 1000;
                    Metrics m = new Metrics();
                    m.uid = USER_TO_START + i;
                    m.day_part = LocalDate.fromMillisSinceEpoch(interval); //add one second;
                    m.ts = new Timestamp(interval);
                    m.strap_id = strapID;
                    m.hr = 100;
                    m.accel_mag = 0.0f;
                    m.accel_x = floatNumbers;
                    m.accel_y = floatNumbers;
                    m.accel_z = floatNumbers;
                    m.rr_0 = floatNumbers;
                    m.rr_1 = floatNumbers;
                    m.rr_2 = floatNumbers;
                    m.rr_3 = floatNumbers;
                    m.rr_4 = floatNumbers;
                    m.sig_error = 1;
                    m.hr_confidence = 1;
                    data.add(m);

                }
                records.add(data);
                totalCount++;
                sleepCount++;
                try{ Thread.sleep(200); } catch (Exception ex){};
            }

            /*try {
                long elapsed = (System.currentTimeMillis() - start);
                long expected = FREQUENCY * 1000 / WAIT;
                // we use 1/4 time to insert data
                if ( elapsed + 1000 < expected)  {
                    //Give sender thread more time as we produce too many
                    System.out.println(("Sleep " + (expected - elapsed)/1000 + " seconds " + ", " + calendar.getTime()));
                    Thread.sleep(expected - elapsed );
                }


            }
            catch (Exception ex){

            }*/
            try{ Thread.sleep(1000 * 60 * 3); } catch (Exception ex){};
            System.out.println(("Finished " + (round+1) + " rounds " + ", " + calendar.getTime()));
        }

        System.out.println(("Finished generating data " + calendar.getTime()));
        System.out.println("Generated  " + totalCount + " records");

    }

    private static class Metrics{
        public int uid;
        public LocalDate day_part;
        public Timestamp ts;
        public float accel_mag;
        public float accel_x;
        public float accel_y;
        public float accel_z;
        public short hr;
        public int hr_confidence;
        public float rr_0;
        public float rr_1;
        public float rr_2;
        public float rr_3;
        public float rr_4;
        public int sig_error;
        public String strap_id;
        public long ttl;
    }

}
