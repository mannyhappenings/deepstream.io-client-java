package io.deepstream.test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.java_websocket.WebSocketImpl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.function.BinaryOperator;
import java.util.logging.Logger;

import io.deepstream.ConfigOptions;
import io.deepstream.ConnectionState;
import io.deepstream.ConnectionStateListener;
import io.deepstream.DeepstreamClient;
import io.deepstream.DeepstreamFactory;
import io.deepstream.DeepstreamRuntimeErrorHandler;
import io.deepstream.Event;
import io.deepstream.EventListener;
import io.deepstream.InvalidDeepstreamConfig;
import io.deepstream.LoginResult;
import io.deepstream.RpcRequestedListener;
import io.deepstream.RpcResponse;
import io.deepstream.RpcResult;
import io.deepstream.Topic;

public class TestClass {
    private static String url = "wss://preprod-io.charcoaleats.com";
    private static Logger logger = Logger.getLogger("Test Class");
    private static SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS");
    private static DeepstreamClient client = null;
    private static boolean loginInProgress = false;
    private static boolean loggedIn = false;
    private static boolean providerAdded = false;
    private static boolean subscriberAdded = false;

    static {
//        WebSocketImpl.DEBUG = true;
    }

    public static <T extends Exception> void logerror(String message, T exception) {
        Date time = new Date();

        System.out.println( format.format(time) + " :: ERROR :: "  + message );
        exception.printStackTrace();
    }

    public static void loginfo(String ... message) {
        Date time = new Date();

        System.out.println( format.format(time) + " :: LOG :: " + Arrays.asList(message).toString());
    }

    public static void main(String[] args) {
        int iterations = 1;
        long start = System.nanoTime();
        for(int i=0; i<iterations; i++) {
            deepstreamTest();
        }
        long end = System.nanoTime();
        loginfo("Timer started at " + start);
        loginfo("Timer stopped at " + end);
        loginfo("Total time for test method " + ((end - start)/iterations) + " nano seconds");

    }

    public static void testAtomic() {
//        AtomicReference<Integer> ar = new AtomicReference<Integer>(123124);
//        Integer x = ar.accumulateAndGet(new Integer(10), new BinaryOperator<Integer>() {
//            @Override
//            public Integer apply(Integer integer, Integer integer2) {
//                return integer * integer2;
//            }
//        });
        Integer i = 10;
        AtomicInteger ai = new AtomicInteger(i);
        increment(ai);

    }

    public static void increment(AtomicInteger ai) {
        ai.incrementAndGet();
//        i++;
    }

    public static void deepstreamTest() {
        loginfo("Testing Deepstream client.");
//        setConnectivityState();
        try {
            client = DeepstreamFactory.getInstance().getClient(getUrl(), getDeepstreamConfig());
            loginfo("Client Initialised with url => " + getUrl() + " and config => " + getDeepstreamConfig());
//            setConnectivityState();
            addConnectionChangeListener();
            addErrorHandler();
//            checkConnectionState();
            loginWithClient();
//            addProvider();
//            checkConnectionState();
//            client.login(getLoginParams());
        } catch (URISyntaxException e) {
            logerror("Invalid url: " + getUrl(), e);
            checkConnectionState();
        } catch (InvalidDeepstreamConfig invalidDeepstreamConfig) {
            logerror("invalid deepstream config", invalidDeepstreamConfig);
            checkConnectionState();
        }
        checkConnectionState();
        loginfo("Deepstream client", client.toString());
        checkConnectionState();
        loginfo("DeepStream connection state", client.getConnectionState().toString());

        loginfo("Starting threads");
        loginfo("Starting connection checker thread.");
        new DeepStreamConnectionChecker(2000).start();
//        loginfo("Starting RPC Maker Thread");
//        new Thread(new RpcMaker(500)).start();
//        loginfo("Staring Internet Checker");
//        new Thread(new InternetChecker(1000)).start();

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                loginfo("Reinitialising connection.");
                client.throwError(new Exception("Test Exception from test class"));
            }
        });

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void checkConnectionState() {
        loginfo("DeepStream connection state", client.getConnectionState().toString());
    }

    private static JsonElement getLoginParams() {
        JsonObject jsonElement = new JsonObject();
        jsonElement.addProperty("cbUserName", "test-user");
        jsonElement.addProperty("token", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ0ZXN0LXVzZXIifQ.LQo-a0_XyFPvIfbx6qU9EmC6kUlEypgF46I_sSWozF4");
        jsonElement.addProperty("isCharcoalUser", true);

        return jsonElement;
    }

    private static void addConnectionChangeListener() {
        client.addConnectionChangeListener(new ConnectionStateListener() {
            @Override
            public void connectionStateChanged(ConnectionState connectionState) {
                loginfo("Connection changed to " + connectionState);
                if (connectionState == ConnectionState.OPEN) {
                    addProvider();
                    addSubscriber();
                }
                if (connectionState == ConnectionState.ERROR) {
                    try {
                        client.reInitialiseConnection();
                    } catch (URISyntaxException e) {
                        e.printStackTrace();
                    }
                }
//                if (connectionState == ConnectionState.AWAITING_AUTHENTICATION) {
//                    loginWithClient();
//                }
//                if (connectionState == ConnectionState.AUTHENTICATING) {
//                    loginfo("State => " + connectionState);
//                }
            }
        });
    }

    private static void loginWithClient() {
        loginInProgress = true;
        loginfo("Trying to login with " + getLoginParams());
        LoginResult loginResponse = client.login(getLoginParams());
        loginfo("Login result => " + loginResponse.loggedIn());
        if (loginResponse.loggedIn()) {
            loggedIn = true;
        }
        loginInProgress = false;
    }

    private static Properties getDeepstreamConfig() {
        Properties deepstreamConfig = new Properties();

//        /** 3 Seconds for rpc timeout */
//        deepstreamConfig.setProperty(ConfigOptions.RPC_RESPONSE_TIMEOUT.toString(), "3000");
//
//        /** 100 Reconnection attempts */
//        deepstreamConfig.setProperty(ConfigOptions.MAX_RECONNECT_ATTEMPTS.toString(), "10000");
//
//        /** Reconnection delay increments */
//        deepstreamConfig.setProperty(ConfigOptions.RECONNECT_INTERVAL_INCREMENT.toString(), "3000");
//
//        /** Reconnection delay attempts */
//        deepstreamConfig.setProperty(ConfigOptions.MAX_RECONNECT_INTERVAL.toString(), "3000");
//
//        /** RPC ACK Timeout */
//        deepstreamConfig.setProperty(ConfigOptions.RPC_ACK_TIMEOUT.toString(), "500");
//
//        /** RPC Response Timeout */
//        deepstreamConfig.setProperty(ConfigOptions.RPC_RESPONSE_TIMEOUT.toString(), "3000");
        deepstreamConfig.setProperty(ConfigOptions.RPC_ACK_TIMEOUT.toString(), "1000");
        deepstreamConfig.setProperty(ConfigOptions.RPC_RESPONSE_TIMEOUT.toString(), "6000");
        deepstreamConfig.setProperty(ConfigOptions.SUBSCRIPTION_TIMEOUT.toString(), "10000");
        deepstreamConfig.setProperty(ConfigOptions.RECORD_READ_TIMEOUT.toString(), "5000");
        deepstreamConfig.setProperty(ConfigOptions.RECORD_READ_TIMEOUT.toString(), "5000");
        deepstreamConfig.setProperty(ConfigOptions.MAX_RECONNECT_INTERVAL.toString(), "2500");
        deepstreamConfig.setProperty(ConfigOptions.MAX_RECONNECT_ATTEMPTS.toString(), "100000");
        deepstreamConfig.setProperty(ConfigOptions.RECONNECT_INTERVAL_INCREMENT.toString(), "500");

//        /** Reconnection delay attempts */
//        deepstreamConfig.setProperty(ConfigOptions.MAX_RECONNECT_INTERVAL.toString(), "3000");

        return deepstreamConfig;
    }

    public static String getUrl() {
        return url;
    }

    private static void addProvider() {
        if (providerAdded) {
            loginfo("Provider already added.");
            return;
        }
        loginfo("Adding provider");
        RpcRequestedListener proderHandler = new RpcRequestedListener() {
            @Override
            public void onRPCRequested(String rpcName, Object data, RpcResponse response) {
                String jsonData = new Gson().toJson(data);
                loginfo("RPC Request Received: rpcName :: " + rpcName, ", data :: " + jsonData);
                response.ack();
            }
        };
        client.rpc.provide("sampleProvider", proderHandler);
        providerAdded = true;
    }

    private static void addSubscriber() {
        if (subscriberAdded) {
            loginfo("Subscriber already added.");
//            try {
//                client.reSubsribe();
//            } catch (URISyntaxException e) {
//                e.printStackTrace();
//            }
            return;
        }
        loginfo("Adding subscriber");

        EventListener subscriptionHandler = new EventListener() {
            @Override
            public void onEvent(String eventName, Object args) {
                loginfo("Received event: " + args);
            }
        };
        client.event.subscribe("test-room-preprod/orderReceived", subscriptionHandler);
        subscriberAdded = true;
    }

    private static void addErrorHandler() {
        client.setRuntimeErrorHandler(new DeepstreamRuntimeErrorHandler() {
            @Override
            public void onException(Topic topic, Event event, String errorMessage) {
                loginfo("Error occurred in deepstream",
                        "Topic=>" + topic,
                        "event=>" + event,
                        "Error Message=>" + errorMessage);
            }
        });
    }

    private static void setConnectivityState() {
        loginfo("Checking internet.");
//        GlobalConnectivityState connectivityState = GlobalConnectivityState.DISCONNECTED;
        try {
            URL checkUrl = new URL("http://google.com");
            URLConnection connection = checkUrl.openConnection();
            connection.connect();
//            connectivityState = GlobalConnectivityState.CONNECTED;
        } catch (IOException e) {
            loginfo("Internet is not connected.");
        }
//        client.setGlobalConnectivityState(connectivityState);
//        loginfo("Set connectivity state to " + connectivityState);
    }

    static class DeepStreamConnectionChecker extends Thread {
        private int timerWait = 2000;

        DeepStreamConnectionChecker(int timerWait) {
            this.timerWait = timerWait;
            this.setName("DeepStreamConnectionChecker");
        }

        @Override
        public void run() {
            Timer timer = new Timer();
            while (true) {
                try {
                    timer.resetClock();
                    RpcResult rpcResult = client.rpc.make("ping", null);
                    loginfo("Total time for rpc request (in millis): " + timer.getElapsedTime() / 1000000.0);
                    loginfo("RPC Response => " + rpcResult.getData().toString());
                    if (rpcResult.success()) {
                        loginfo("Connection ALIVE.");
                    } else {
                        loginfo("Connection unavailable.");
                    }
                    try {
                        Thread.sleep(timerWait);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch(Throwable t) {
                    logerror("Error while checking connection", new Exception(t));
                }
            }
        }
    }
    static class RpcMaker extends Thread {
        private int timerWait = 500;

        RpcMaker(int timerWait) {
            this.timerWait = timerWait;
            this.setName("RpcMaker");
        }

        @Override
        public void run() {
            Timer timer = new Timer();
            while(true) {
                loginfo("Making rpc request");
                timer.resetClock();
                RpcResult rpcRespose = client.rpc.make("sampleProvider", "Sample data :P");
                loginfo("Total time for rpc request (in nanoseconds): " + timer.getElapsedTime());
                loginfo("RPC Response => " + rpcRespose.getData().toString());
                try {
                    loginfo("RPC Thread sleeping...");
                    Thread.sleep(timerWait);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    static class InternetChecker extends Thread {
        private int timerWait = 1000;

        InternetChecker(int timerWait) {
            this.timerWait = timerWait;
            this.setName("InternetChecker");
        }

        @Override
        public void run() {
            while (true) {
                loginfo("Checking internet");
                setConnectivityState();
                try {
                    Thread.sleep(timerWait);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class Timer {
        private long start;

        public Timer() {
            this.start = System.nanoTime();
        }

        public void resetClock() {
            this.start = System.nanoTime();
        }
        public long getStart() {
            return this.start;
        }
        public long getElapsedTime() {
            return System.nanoTime() - start;
        }
    }
}
