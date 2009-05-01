// This software code is made available "AS IS" without warranties of any
// kind.  You may copy, display, modify and redistribute the software
// code either by itself or as incorporated into your code; provided that
// you do not remove any proprietary notices.  Your use of this software
// code is at your own risk and you waive any claim against Amazon
// Digital Services, Inc. or its affiliates with respect to your use of
// this software code. (c) 2006 Amazon Digital Services, Inc. or its
// affiliates.

package com.amazon.s3shell;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple shell-like program for interacting with S3 from the command-line.
 * This class is primary designed to interact with the user at the terminal,
 * but can read commands from any provided InputStream.
 * <p>
 * Once instantiated, this class has two modes.  Initially it can be in
 * "connected" mode or "disconnected" mode.  This has no direct relation on
 * whether or not a TCP connection exists to the S3 server at any point, it
 * merely refers to whether enough information has been provided by the user
 * to attempt to make a connection.  Once a host, username, and password have
 * been provided through the constructor or through issuing commands, the
 * instance becomes "connected" and will begin communicating with S3 as
 * subsequent commands are issued.  When the instance is "disconnected", the
 * only commands it will accept are those that don't interact with the S3
 * server at all.  Once "connected", any command may be executed.
 * <p>
 * For a list of commands accepted by the shell, see the S3Shell wiki node.
 * <p>
 * When this class is run from the command line, it will initialize the S3
 * connection parameters from the command line arguments and then begin
 * reading commands from standard input.
 * <p>
 * Instances of this class are <b>NOT</b> safe for concurrent access by
 * multiple threads.
 *
 * @author Grant Emery (c) 2006 Amazon.com
 **/
public class s3sh {
    /** Represents commands that may be executed when "disconnected" */
    private static final Set<String> NO_CONNECTION_COMMANDS = new HashSet<String>();
    static {
        NO_CONNECTION_COMMANDS.add("help");
        NO_CONNECTION_COMMANDS.add("quit");
        NO_CONNECTION_COMMANDS.add("exit");
        NO_CONNECTION_COMMANDS.add("host");
        NO_CONNECTION_COMMANDS.add("user");
        NO_CONNECTION_COMMANDS.add("pass");
        NO_CONNECTION_COMMANDS.add("threads");
        NO_CONNECTION_COMMANDS.add("time");
    }

    /**
     * Represents the different execution time reporting modes available.
     * "LONG" commands are commands that run longer than five seconds.
     **/
    public enum TimingMode { NONE, LONG, ALL };
    /** A long command runs for longer than five seconds */
    private static final int LONG_COMMAND = 5000;

    /** Number of times to attempt executing commands where we retry. */
    private static final int MAX_RETRIES = 3;
    /** Number of milliseconds to wait between retry attempts. */
    private static final int RETRY_SLEEP = 5000;
    /** Maximum number of threads allowable for multithreaded commands */
    public static final int MAX_THREADS = 20;
    /** When running "deleteall", print a period for this many deletions */
    private static final int DELETES_PER_DOT = 10;

    /** The current S3 host to connect to. */
    private String m_host;
    /** The current S3 user to connect as. */
    private String m_user;
    /** The current S3 password to connect with. */
    private String m_pass;
    /** The current S3 bucket to operate on. */
    private String m_bucket;

    /** The current prompt string displayed on the terminal before
     *  reading a line of input */
    private String m_prompt;
    /** The number of threads to execute multithreaded commands with */
    private int m_threads;
    /** The current timing mode for display of command timing information */
    private TimingMode m_timingMode;

    /** The current S3Store for communicating with S3 */
    private S3Store m_store;
    
    /**
     * Command-line entry point for running s3sh.  If present, the
     * command-line arguments will be assigned to host, username, password,
     * and bucket in that order.  The shell will then be started to read from
     * standard input.
     **/
    public static void main(String argv[]) throws IOException {
        String host = null;
        String user = null;
        String pass = null;
        String bucket = null;
        if(argv.length > 0) host = argv[0];
        if(argv.length > 1) user = argv[1];
        if(argv.length > 2) pass = argv[2];
        if(argv.length > 3) bucket = argv[3];
        if(argv.length > 4) {
            System.err.println("S3sh [host] [accesskey] [secretaccessid] [bucket]");
            System.exit(1);
        }

        s3sh shell = new s3sh(host, user, pass, bucket);
        shell.setPrompt("> ");

        try {
            shell.processCommands(System.in);
        }
        catch(IOException e) {
            System.out.println("error reading commands: "+e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Constructor to assign connection parameters for using S3.  Any of these
     * may be null, the shell will not allow connections to S3 or certain
     * operations until they have been set to something non-null.
     *
     * @param host The S3 host to use [may be null]
     * @param user The Amazon Web Services Access Key to use [may be null]
     * @param pass The Amazon Web Services Secret Key ID to use [may be null]
     * @param bucket The S3 bucket to operate on [may be null]
     **/
    public s3sh(final String host, final String user, final String pass, final String bucket) {
        m_host = host;
        m_user = user;
        m_pass = pass;
        m_bucket = bucket;

        m_prompt = "";
        m_threads = 1;
        m_timingMode = TimingMode.LONG;
    }

    /**
     * Executes the main read-execute-print loop of the shell.  Commands will
     * be read from the given stream and results will be written to standard 
     * output and standard error.
     *
     * @param commandStream The stream of commands to read [may not be null]
     * @throws IOException On failure to read from the command stream
     **/
    public void processCommands(InputStream commandStream) throws IOException {
        reinitializeStore();

        BufferedReader br = new BufferedReader(new InputStreamReader(commandStream));
        String line;
        while((line = getLine(br)) != null) {
            StringTokenizer st = new StringTokenizer(line);

            if(st.countTokens() == 0) {
                continue;
            }

            String cmd = st.nextToken();

            // If we don't have enough information to connect to S3 yet, only
            // allow a restricted subset of commands to be run.
            if(m_store == null && !NO_CONNECTION_COMMANDS.contains(cmd)) {
                System.out.println("not yet connected to S3; set host, user, and pass to continue");
                continue;
            }

            long starttime = System.currentTimeMillis();
            try {
                if(cmd.equals("bucket")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: bucket [name]");
                        continue;
                    }

                    if(st.hasMoreTokens()) {
                        setBucket(st.nextToken());
                    } else {
                        if(getBucket() != null) {
                            System.out.println("bucket = "+getBucket());
                        } else {
                            System.out.println("bucket is not set");
                        }
                    }
                } else if(cmd.equals("count")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: count [prefix]");
                        continue;
                    }

                    String prefix = null;
                    if(st.countTokens() > 0) {
                        prefix = st.nextToken();
                    }

                    int count = countItems(prefix);

                    // On error, countItems returns < 0
                    if(count >= 0) {
                        System.out.println(count + " item(s)");
                    }
                } else if(cmd.equals("createbucket")) {
                    if(st.countTokens() != 0) {
                        System.out.println("error: createbucket");
                        continue;
                    }

                    boolean ok = m_store.createBucket();

                    if(!ok) {
                        System.out.println("error: unable to create bucket");
                    }
                } else if(cmd.equals("delete")) {
                    if(st.countTokens() != 1) {
                        System.out.println("error: delete <id>");
                        continue;
                    }

                    boolean ok = m_store.deleteItem(st.nextToken());

                    if(!ok) {
                        System.out.println("error: unable to delete item");
                    }
                } else if(cmd.equals("deleteall")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: deleteall [prefix]");
                        continue;
                    }

                    String prefix = null;
                    if(st.countTokens() > 0) {
                        prefix = st.nextToken();
                    }

                    int deletecount = deleteAll(prefix);

                    System.out.println("deleted "+deletecount+" item(s)");
                } else if(cmd.equals("deletebucket")) {
                    if(st.countTokens() != 0) {
                        System.out.println("error: deletebucket");
                        continue;
                    }

                    boolean ok = m_store.deleteBucket();

                    if(!ok) {
                        System.out.println("error: unable to delete bucket");
                    }
                } else if(cmd.equals("exit") || cmd.equals("quit")) {
                    return;
                } else if(cmd.equals("get")) {
                    if(st.countTokens() != 1) {
                        System.out.println("error: get <id>");
                        continue;
                    }

                    byte[] data = m_store.getItem(st.nextToken());

                    if(data == null) {
                        System.out.println("item not found");
                    } else {
                        System.out.println(new String(data));
                    }
                } else if(cmd.equals("getfile")) {
                    if(st.countTokens() != 2) {
                        System.out.println("error: getfile <id> <file>");
                        continue;
                    }

                    byte[] data = m_store.getItem(st.nextToken());
                    if(data == null) {
                        System.out.println("item not found");
                    } else {
                        FileOutputStream datafile = new FileOutputStream(st.nextToken());
                        try {
                            datafile.write(data);
                        }
                        finally {
                            datafile.close();
                        }
                    }
                } else if(cmd.equals("help")) {
                    printHelp();
                } else if(cmd.equals("host")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: host [hostname]");
                        continue;
                    }

                    if(st.hasMoreTokens()) {
                        setHost(st.nextToken());
                    } else {
                        if(getHost() != null) {
                            System.out.println("host = "+getHost());
                        } else {
                            System.out.println("host is not set");
                        }
                    }
                } else if(cmd.equals("list")) {
                    if(st.countTokens() > 3) {
                        System.out.println("error: list [prefix] [marker] [max]");
                    }

                    String prefix = null;
                    String marker = null;
                    int max = 0;
                    if(st.hasMoreTokens()) {
                        prefix = st.nextToken();
                    }
                    if(st.hasMoreTokens()) {
                        marker = st.nextToken();
                    }
                    if(st.hasMoreTokens()) {
                        try {
                            max = Integer.parseInt(st.nextToken());
                        }
                        catch(NumberFormatException e) {
                            System.out.println(e.getMessage());

                            continue;
                        }

                        if(max < 0) {
                            System.out.println("error: max must be >= 0");
                            continue;
                        }
                    }

                    // This only retrieves the first "page" of items from
                    // the server.
                    List<String> ids = m_store.listItems(prefix, marker, max);
                    if(ids != null) {
                        for(String id : ids) {
                            System.out.println(id);
                        }
                    }
                } else if(cmd.equals("listbuckets")) {
                    if(st.countTokens() != 0) {
                        System.out.println("error: listbuckets");
                        continue;
                    }

                    List<String> buckets = m_store.listBuckets();
                    if(buckets != null) {
                        for(String bucket : buckets) {
                            System.out.println(bucket);
                        }
                    }
                } else if(cmd.equals("pass")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: pass [username]");
                        continue;
                    }

                    if(st.hasMoreTokens()) {
                        setPass(st.nextToken());
                    } else {
                        if(getPass() != null) {
                            System.out.println("pass = "+getPass());
                        } else {
                            System.out.println("pass is not set");
                        }
                    }
                } else if(cmd.equals("put")) {
                    if(st.countTokens() < 2) {
                        System.out.println("error: put <id> <data>");
                        continue;
                    }

                    String id = st.nextToken();
                    String restOfLine = line.substring(line.indexOf(id) + id.length() + 1);

                    boolean ok = m_store.storeItem(id, restOfLine.getBytes());

                    if(!ok) {
                        System.out.println("error: unable to store item");
                    }
                } else if(cmd.equals("putfile")) {
                    if(st.countTokens() != 2) {
                        System.out.println("error: putfile <id> <file>");
                        continue;
                    }

                    boolean ok = putFile(st.nextToken(), st.nextToken());

                    if(!ok) {
                        System.out.println("error: unable to store item");
                    }
                } else if(cmd.equals("threads")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: threads [num]");
                        continue;
                    }

                    if(st.hasMoreTokens()) {
                        try {
                            setThreads(Integer.parseInt(st.nextToken()));
                        }
                        catch(NumberFormatException e) {
                            System.out.println(e.getMessage());
                        }
                    } else {
                        System.out.println("threads = "+getThreads());
                    }
                } else if(cmd.equals("time")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: time [none|long|all]");
                        continue;
                    }

                    if(st.hasMoreTokens()) {
                        final String mode = st.nextToken();
                        try {
                            setTimingMode(TimingMode.valueOf(TimingMode.class, mode.toUpperCase()));
                        }
                        catch(IllegalArgumentException e) {
                            System.out.println("error: time [none|long|all]");
                        }
                    } else {
                        System.out.println("time = "+getTimingMode());
                    }
                } else if(cmd.equals("user")) {
                    if(st.countTokens() > 1) {
                        System.out.println("error: user [username]");
                        continue;
                    }

                    if(st.hasMoreTokens()) {
                        setUser(st.nextToken());
                    } else {
                        if(getUser() != null) {
                            System.out.println("user = "+getUser());
                        } else {
                            System.out.println("user is not set");
                        }
                    }
                } else {
                    System.out.println("error: unknown command");
                }
            }
            catch(IOException e) {
                System.out.println(e.getMessage());
            }
            catch(IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }

            long runtime = System.currentTimeMillis() - starttime;
            if(m_timingMode == TimingMode.ALL || m_timingMode == TimingMode.LONG && runtime > LONG_COMMAND) {
                System.out.println(formatRuntime(runtime));
            }
        }
    }

    /**
     * Returns the current timing mode of the shell.
     *
     * @return The current timing mode of the shell [not null].
     * @see TimingMode
     **/
    public TimingMode getTimingMode() {
        return m_timingMode;
    }
    /**
     * Sets the current timing mode of the shell.
     *
     * @param timingMode The timing mode to use [may not be null]
     * @see TimingMode
     */
    public void setTimingMode(final TimingMode timingMode) {
        if(timingMode == null) throw new IllegalArgumentException("timingMode may not be null");

        m_timingMode = timingMode;
    }

    /**
     * Gets the number of threads used by the shell to execute multithreaded
     * commands.
     *
     * @return The number of threads [between 1 and MAX_THREADS, inclusive]
     * @see #MAX_THREADS
     */
    public int getThreads() {
        return m_threads;
    }
    /**
     * Sets the number of threads used by the shell to execute multithreaded
     * commands.
     *
     * @param threads The number of threads [between 1 and MAX_THREADS,
     * inclusive]
     * @see #MAX_THREADS
     */
    public void setThreads(final int threads) {
        if(threads < 1 || threads > MAX_THREADS) {
            throw new IllegalArgumentException("number of threads must be between 1 and "+MAX_THREADS);
        }
        m_threads = threads;
    }

    /**
     * Gets the prompt displayed to the user before reading a line of input.
     *
     * @return The current prompt [not null]
     */
    public String getPrompt() {
        return m_prompt;
    }
    /**
     * Sets the prompt displayed to the user before reading a line of input.
     *
     * @param prompt The prompt to display [not null]
     */
    public void setPrompt(final String prompt) {
        if(prompt == null) throw new IllegalArgumentException("prompt may not be null");

        m_prompt = prompt;
    }

    /**
     * Gets the current S3 host used to execute commands.
     *
     * @return The current host [may be null]
     */
    public String getHost() {
        return m_host;
    }
    /**
     * Sets the current S3 host used to execute commands.
     *
     * @param host The S3 host [may be null]
     */
    public void setHost(final String host) {
        m_host = host;

        reinitializeStore();
    }

    /**
     * Gets the current AWS Access Key ID to use to connect to S3.
     *
     * @return The current Access Key ID [may be null]
     */
    public String getUser() {
        return m_user;
    }
    /**
     * Sets the current AWS Access Key ID to use to connect to S3.
     *
     * @param user The Access Key ID [may be null]
     */
    public void setUser(final String user) {
        m_user = user;

        reinitializeStore();
    }

    /**
     * Gets the current AWS Secret Access Key to use to connect to S3.
     *
     * @return The current AWS Secret Access Key [may be null]
     */
    public String getPass() {
        return m_pass;
    }
    /**
     * Sets the AWS Secret Access Key to use to connect to S3.
     *
     * @param pass The Secret Access Key [may be null]
     */
    public void setPass(final String pass) {
        m_pass = pass;

        reinitializeStore();
    }

    /**
     * Gets the current S3 bucket.
     *
     * @return The current S3 bucket [may be null]
     */
    public String getBucket() {
        return m_bucket;
    }
    /**
     * Sets the current S3 bucket.
     *
     * @param bucket The bucket to use [may be null]
     */
    public void setBucket(final String bucket) {
        m_bucket = bucket;

        if(m_store != null) m_store.setBucket(bucket);
    }

    /**
     * Creates a new S3Store whenever the connection parameters change.  If
     * any required parameter is null, the S3Store will be set to null.
     */
    private void reinitializeStore() {
        if(m_host != null && m_user != null && m_pass != null) {
            m_store = new S3Store(m_host, m_user, m_pass, m_bucket);
        } else {
            m_store = null;
        }
    }

    /**
     * Reads the next line from the given reader.  Before reading the line,
     * the current prompt will be printed to System.out.
     *
     * @param br The BufferedReader to read from [may not be null]
     * @see #setPrompt(String)
     * @see #getPrompt()
     */
    private String getLine(final BufferedReader br) throws IOException {
        System.out.print(m_prompt);
        return br.readLine();
    }

    /**
     * Converts a length of time in milliseconds into a nicely formatted 
     * string representing hours, minutes, seconds, and milliseconds.
     * <p>
     * For example, <tt>43384005</tt> becomes <tt>12h03m04.005s</tt>.
     *
     * @param runtime The time to format
     * @return A string representation of the time suitable for user display
     */
    private String formatRuntime(long runtime) {
        final StringBuilder timestr = new StringBuilder();
        // First compute the number of hours
        if(runtime >= 60 * 60 * 1000) {
            timestr.append(runtime / (60 * 60 * 1000)).append("h");
            runtime %= 60 * 60 * 1000;
        }

        // If there was a non-zero number of hours, or if there are minutes to
        // include, we need to format the number of minutes.  If there are
        // hours, we need to use a zero-padded number of minutes.
        if(runtime >= 60 * 1000 || timestr.length() > 0) {
            String mins;
            if(timestr.length() > 0) {
                timestr.append(String.format("%02d", runtime / (60 * 1000)));
            } else {
                timestr.append(runtime / (60 * 1000));
            }
            timestr.append("m");

            runtime %= 60 * 1000;
        }

        // Similarly for the number of seconds, but even if the runtime is
        // less than one second, we want to at least include a zero before the
        // decimal point.
        if(timestr.length() > 0) {
            timestr.append(String.format("%02d", runtime / 1000));
        } else {
            timestr.append(runtime / 1000);
        }
        timestr.append(".");

        runtime %= 1000;

        // And finally, the milliseconds, always zero-padded.
        timestr.append(String.format("%03d", runtime)).append("s");

        return timestr.toString();
    }

    /**
     * Helper method to store a the contents of a file into S3 under a given
     * ID.
     *
     * @param id The S3 ID to store the contents of the file under [may not be
     * null]
     * @param file The name of the file to read [may not be null]
     * @throws IOException Thrown by the file operations or S3 on error.
     */
    private boolean putFile(final String id, final String file) throws IOException {
        File datafile = new File(file);

        if(datafile.length() > Integer.MAX_VALUE) {
            System.out.println(datafile+" is too large");
            return false;
        }
        byte[] buf = new byte[(int)datafile.length()];

        DataInputStream dis = new DataInputStream(new FileInputStream(datafile));
        try {
            dis.readFully(buf);
        }
        finally {
            dis.close();
        }

        return m_store.storeItem(id, buf);
    }

    /**
     * Helper method to iteratively count the items in a bucket.
     *
     * @param prefix If non-null, only items with IDs start with this prefix
     * are counted [may be null]
     * @return The number of matching items.  On error, a negative number will
     * be returned.
     */
    private int countItems(final String prefix) throws IOException {
        String lastid = prefix;
        int count = 0;
        List<String> ids;
        while(true) {
            ids = m_store.listItems(prefix, lastid);

            if(ids == null) {
                System.out.println("error: unable to count bucket contents");
                return -1;
            } else if(ids.size() == 0) {
                break;
            }

            count += ids.size();
            lastid = ids.get(ids.size() - 1);
        }

        return count;
    }

    /**
     * Helper method to delete all the items in a bucket.  This method will
     * repeatedly list the bucket contents and delete the resulting items
     * until no more matching items are found.
     *
     * @param prefix If non-null, only items with IDs that have this prefix
     * will be deleted [may be null]
     * @return The number of items deleted
     */
    private int deleteAll(final String prefix) {
        // This code isn't implemented with a ThreadPoolExecutor because 
        // there isn't any way to make a ThreadPoolExecutor that is (1) bounded
        // in the number of threads, (2) has bounded queue length, and (3)
        // causes task submitters to block when the queue is full.  We want
        // feature (1) to prevent overloading of S3, (2) to keep from running
        // out of memory if there are many IDs to delete, and (3) because it
        // simplifies our design.  We could get close to this by using a
        // ThreadPoolExecutor with a CallerRunsPolicy, but that makes it
        // difficult to have a single deleter thread.
        final AtomicInteger deleteCount = new AtomicInteger(0);

        final BlockingQueue<String> deleteQueue = new ArrayBlockingQueue<String>(100 * m_threads);

        final Thread[] deleters = new Thread[m_threads];
        for(int threadidx = 0; threadidx < m_threads; ++threadidx) {
            deleters[threadidx] = new Thread(new ItemDeleter(new S3Store(m_store), deleteQueue, Thread.currentThread(), deleters, deleteCount), "delete-items-"+threadidx);
        }
        for(Thread t : deleters) {
            t.start();
        }

        // This is the main loop that will read IDs from S3 and populate the
        // queue with them for the deleter threads to pick up.  If any of
        // the deleters encounter errors, they will interrupt us to abort
        // processing, so stop if we get interrupted.
        String lastid = null;
POPULATE:
        while(!Thread.interrupted()) {
            List<String> ids = null;
            try {
                ids = listItemsWithRetry(m_store, prefix, lastid, 0);
            }
            catch(IOException e) {
                e.printStackTrace();
            }

            if(ids == null) {
                // Ids will be null if listItems returned null to indicate
                // an error, or if it threw an IOException.  Abort operation
                // and shut down all the deleter threads.
                for(Thread t : deleters) {
                    t.interrupt();
                }

                break;
            }

            // If no items were returned by listing, there are no more
            // items in the bucket.  Poison the work queue with an empty
            // string so the deleters will know to return.  This could block
            // if the work queue is at capacity so it's possible we'd get
            // interrupted while waiting if there is an error.  We don't
            // need to retry, however, because if this thread is interrupted,
            // all the deleter threads will be interrupted as well and will
            // know to abort.
            if(ids.size() == 0) {
                try {
                    deleteQueue.put("");
                }
                catch(InterruptedException e) { }
                break;
            }

            // Otherwise there are still some items left.  Add them one at
            // a time into the work queue for the deleters.  This will likely
            // block from time to time.
            for(String id : ids) {
                try {
                    deleteQueue.put(id);
                }
                catch(InterruptedException e) {
                    // If any thread encounters an error, it will interrupt
                    // all other working threads including us.
                    break POPULATE;
                }
            }
            
            // Finally, update the last id so we can continue paging through
            // the bucket contents
            lastid = ids.get(ids.size() - 1);
        }

        // Wait for all the deleter threads to finish up.  If we're here,
        // We've either read all the IDs from S3 and poisoned the queue,
        // we encountered an error and interrupted all the deleter threads,
        // or we were interrupted ourselves.  Whichever way, we should wait for
        // all the deleter threads to exit.
        for(Thread t : deleters) {
            // Other threads may still be processing requests when we get here.
            // if they encounter errors, they will interrupt us, but we don't
            // want that to stop us from joining every thread.
            boolean joined = false;
            while(!joined) {
                try {
                    t.join();
                    joined = true;
                }
                catch(InterruptedException e) { }
            }
        }
        //
        // If any dots were printed, add a new line
        if(deleteCount.get() >= DELETES_PER_DOT) System.out.println();

        return deleteCount.get();
    }

    /**
     * Helper method to print the help message.
     */
    private void printHelp() {
        System.out.println("bucket [bucketname]");
        System.out.println("count [prefix]");
        System.out.println("createbucket");
        System.out.println("delete <id>");
        System.out.println("deleteall [prefix]");
        System.out.println("deletebucket");
        System.out.println("exit");
        System.out.println("get <id>");
        System.out.println("getfile <id> <file>");
        System.out.println("host [hostname]");
        System.out.println("list [prefix] [marker] [max]");
        System.out.println("listbuckets");
        System.out.println("pass [password]");
        System.out.println("put <id> <data>");
        System.out.println("putfile <id> <file>");
        System.out.println("quit");
        System.out.println("time [none|long|all]");
        System.out.println("threads [num]");
        System.out.println("user [username]");
    }

    /**
     * Wrapper around the listItems call of S3Store to add some retry logic.
     * Calls that result in IOExceptions will retry a fixed number of times
     * after a short sleep.
     *
     * @param store The S3Store to use
     * @param prefix The prefix for listing items
     * @param marker The marker for listing items
     * @param max The maximum number of items to return
     * @return The list of item IDs
     * @see S3Store#listItems(String,String,int)
     */
    private static List<String> listItemsWithRetry(S3Store store, String prefix, String marker, int max) throws IOException {
        IOException exception = null;

        for(int tries = 0; tries < MAX_RETRIES; ++tries) {
            try {
                return store.listItems(prefix, marker, max);
            }
            catch(IOException e) {
                try {
                    Thread.sleep(RETRY_SLEEP);
                }
                catch(InterruptedException ie) {
                    throw e;
                }

                exception = e;
            }
        }

        IOException toThrow = new IOException("failed "+MAX_RETRIES+" times: "+exception.getMessage());
        toThrow.initCause(exception);
        throw toThrow;
    }

    /**
     * Wrapper around the deleteItem call of S3Store to add some retry logic.
     * Calls that result in IOExceptions will retry a fixed number of times
     * after a short sleep.
     *
     * @param store The S3Store to use
     * @param id The ID of the item to delete
     * @return True on success, false on failre
     * @see S3Store#deleteItem(String)
     */
    private static boolean deleteItemWithRetry(S3Store store, String id) throws IOException {
        IOException exception = null;

        for(int tries = 0; tries < MAX_RETRIES; ++tries) {
            try {
                return store.deleteItem(id);
            }
            catch(IOException e) {
                try {
                    Thread.sleep(RETRY_SLEEP);
                }
                catch(InterruptedException ie) {
                    throw e;
                }

                exception = e;
            }
        }

        IOException toThrow = new IOException("failed "+MAX_RETRIES+" times: "+exception.getMessage());
        toThrow.initCause(exception);
        throw toThrow;
    }

    /**
     * Helper class to delete items.  This thread consumes items from the
     * given Queue and deletes them from S3 until the Queue is "poisoned",
     * indicating no more work is available.  Upon unrecoverable error, this
     * Thread interrupts all the other threads so the delete process can be
     * aborted.
     */
    private static class ItemDeleter implements Runnable {
        /** An S3 store this thread can execute against */
        private final S3Store m_store;
        /** The queue to read IDs from */
        private final BlockingQueue<String> m_deleteQueue;
        /** The other deleter threads (including this one) to interrupt on
         * error */
        private final Thread[] m_deleters;
        /** The listing thread, also to be interrupted on error */
        private final Thread m_lister;
        /** Reference to a shared integer that holds the count of items
         * deleted so far */
        private final AtomicInteger m_deleteCount;

        /**
         * Basic constructor to create a new item deleter.  The array of
         * the other deleter threads need not be populated when this Thread is
         * created, but it must be populated before this thread is started.
         *
         * @param store The S3 store to use [may not be null]
         * @param deleteQueue The Queue to read IDs from [may not be null]
         * @param lister The thread listing items for deletion from S3 [may
         * not be null]
         * @param deleters The other threads performing deletions [may not be
         * null, should not be empty]
         * @param deleteCount Reference to a shared integer for storing the
         * deleted item count.
         **/
        public ItemDeleter(final S3Store store, final BlockingQueue<String> deleteQueue, final Thread lister, final Thread[] deleters, final AtomicInteger deleteCount) {
            m_store = store;
            m_deleteQueue = deleteQueue;
            m_lister = lister;
            m_deleters = deleters;
            m_deleteCount = deleteCount;
        }

        /**
         * Begins the deletion process.  IDs will be read and sent to S3 for
         * deletion until the queue is poisoned with an empty string or
         * another Thread interrupts this thread because of an error.
         */
        public void run() {
            // If other threads have errors, they will interrupt this thread.
            // If that happens, return as soon as possible.
            while(!Thread.interrupted()) {
                String idToDelete;
                try {
                    idToDelete = m_deleteQueue.take();
                }
                catch(InterruptedException e) {
                    // If we were interrupted, some other thread had an
                    // error.  Abort and return.
                    break;
                }
                // An empty string is a poison object that indicates there
                // is no more work to do.  If we get one, replace it (to kill
                // the next thread) and return.  Replacing it should never
                // block because it will only be present when the lister is
                // done listing objects and there are no other writers.  In
                // any event, if it does and we get interrupted, return
                // anyway, since all threads will be interrupted and will
                // abort.
                if("".equals(idToDelete)) {
                    try {
                        m_deleteQueue.put("");
                    }
                    catch(InterruptedException e) { }

                    return;
                }

                boolean success = false;
                try {
                    success = deleteItemWithRetry(m_store, idToDelete);
                }
                catch(IOException e) {
                    System.out.println(e.getMessage());
                }

                // Now, if we had an error, we have to interrupt everyone
                // else so they also know to abort and return.
                if(!success) {
                    for(Thread t : m_deleters) {
                        if(t != Thread.currentThread()) {
                            t.interrupt();
                        }
                    }
                    m_lister.interrupt();

                    break;
                }

                // Otherwise the item was successfully deleted.  Update the
                // deletion count and print a status dot if needed.
                if(m_deleteCount.incrementAndGet() % DELETES_PER_DOT == 0) {
                    System.out.print(".");
                }
            }
        }
    }
}
