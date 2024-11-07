import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Publisher {
    private static final int MAX_MESSAGE_LENGTH = 100;
    private final String name;
    private String host;
    private int port;
    private final Map<UUID, String> topics = new ConcurrentHashMap<>();
    private final BufferedReader consoleReader;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final int RETRY_DELAY_MS = 5000;
    private static final int CONNECTION_TIMEOUT_MS = 5000;

    public Publisher(String name) {
        this.name = name;
        this.consoleReader = new BufferedReader(new InputStreamReader(System.in));
    }

    public void start() {
        try {
            queryDirectoryService();
            connectToBroker();
            runConsoleMenu();
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            disconnect();
        }
    }

    private void queryDirectoryService() throws IOException {
        try (Socket directorySocket = new Socket("localhost", 6000);
             PrintWriter out = new PrintWriter(directorySocket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(directorySocket.getInputStream()))) {
    
            out.println("QUERY_BROKERS");
            String response = in.readLine();
            if (response != null && !response.isEmpty()) {
                String[] brokers = response.split(",");
                if (brokers.length > 0) {
                    String[] brokerInfo = brokers[0].split(":");
                    host = brokerInfo[0];
                    port = Integer.parseInt(brokerInfo[1]);
                    System.out.println("Connecting to broker at " + host + ":" + port);
                } else {
                    throw new IOException("No brokers available");
                }
            } else {
                throw new IOException("Failed to query brokers");
            }
        }
    }

    private void connectToBroker() throws IOException {
        int attempts = 0;
        while (attempts < MAX_RETRY_ATTEMPTS) {
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_MS);
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send initialization message
                out.println("PUBLISHER:" + name);
                
                String response = waitForResponse();
                
                if (response == null) {
                    throw new IOException("No response from broker");
                } else if (response.startsWith("ERROR:")) {
                    throw new IOException("Failed to connect: " + response);
                } else if (response.startsWith("SUCCESS:")) {
                    System.out.println("Connected to broker at " + host + ":" + port);
                    return; // Successful connection
                } else {
                    throw new IOException("Unexpected response from broker: " + response);
                }
            } catch (IOException e) {
                System.out.println("Connection attempt failed in catch: " + e.getMessage());
                attempts++;
                if (attempts >= MAX_RETRY_ATTEMPTS) {
                    throw new IOException("Failed to connect after " + MAX_RETRY_ATTEMPTS + " attempts: " + e.getMessage());
                }
                System.out.println("Connection attempt failed. Retrying in " + (RETRY_DELAY_MS / 1000) + " seconds...");
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Connection interrupted", ie);
                }
            }
        }
    }
    
    private String waitForResponse() throws IOException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < CONNECTION_TIMEOUT_MS) {
            if (in.ready()) {
                return in.readLine();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for response", e);
            }
        }
        return null; // Timeout occurred
    }

    private void runConsoleMenu() throws IOException {
        while (true) {
            System.out.println("Please select command: create, publish, show, delete, exit.");
            String choice = consoleReader.readLine();
            String[] parts = choice.split(" ", 3);
            String messageType;
            UUID topicId = null;
            String content = null;
            String topicName = null;
            if (parts.length == 3){
                if (parts[0].equals("create")){
                    System.out.println(" argumentserror");
                    runConsoleMenu();
                }
                messageType = parts[0];
                topicId = UUID.fromString(parts[1]);
                content = parts[2];
            }else if (parts.length == 2){
                messageType = parts[0];
                topicName = parts[1];
            }else{
                messageType = choice;
            }
            
            switch (messageType) {
                case "create":
                    System.out.println(parts.length);
                    for (String part : parts){
                        System.out.println(part);
                    }
                    if (parts.length != 2){
                        System.out.println("arguments error");
                        break;
                    }

                    if (topicName == null || topicName.trim().isEmpty()) {
                        System.out.println("Topic name cannot be empty. Please enter a valid name.");
                        break;
                    }
                    try{
                        createNewTopic(topicName);
                    }catch (IOException e){
                        System.out.println("Error creating topic: " + e.getMessage());
                    }
                    
                    break;
                case "publish":
                    System.out.println(parts.length);
                    for (String part : parts){
                        System.out.println(part);
                    }
                    if (parts.length != 3){
                        System.out.println("arguments error");
                        break;
                    }
                    if (topicId == null || !topics.containsKey(topicId)){
                        System.out.println("Topic not found.");
                        break;
                    }
                    if(content.length() > MAX_MESSAGE_LENGTH){
                        System.out.println("Message too long. Truncating to 100 characters.");
                        break;
                    }
                    if (content == null || content.trim().isEmpty()) {
                        System.out.println("Content cannot be empty. Please enter a valid message.");
                        break;
                    }
                    try{
                        publishMessage(topicId, content);
                    }catch (IOException e){
                        System.out.println("Error publishing message: " + e.getMessage());
                    }
                    break;
                case "show":
                    showSubscriberCount();
                    break;
                case "delete":
                    deleteTopic();
                    break;
                case "exit":
                    disconnect();
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    private synchronized void createNewTopic(String topicName) throws IOException {
        UUID topicId = UUID.randomUUID();
        String createTopicMessage = "CREATETOPIC:" + topicId + ":" + topicName;
        
        System.out.println("Sending create topic request: " + createTopicMessage);
        out.println(createTopicMessage);
        synchronized(socket){
            String response = in.readLine();
            System.out.println("Received response: " + response);
            
            if (response.startsWith("SUCCESS:")) {
                topics.put(topicId, topicName);
                System.out.println("Topic created successfully.");
                System.out.println("Topic Name: " + topicName);
                System.out.println("Topic ID: " + topicId);
            } else if (response.startsWith("ERROR:")) {
                System.out.println("Failed to create topic: " + response.substring(6));
            } else {
                System.out.println("Unexpected response from broker: " + response);
            }
    }
    }

    private synchronized void publishMessage(UUID topicId, String message) throws IOException {
        if (topics.isEmpty()) {
            System.out.println("No topics available. Create a topic first.");
            return;
        }

        // System.out.println("Available topics:");
        // for (Map.Entry<UUID, String> entry : topics.entrySet()) {
        //     System.out.println(entry.getKey() + ": " + entry.getValue());
        // }

        // System.out.print("Enter topic ID: ");
        // String topicIdStr = consoleReader.readLine();
        // UUID topicId;
        // try {
        //     topicId = UUID.fromString(topicIdStr);
        // } catch (IllegalArgumentException e) {
        //     System.out.println("Invalid topic ID.");
        //     return;
        // }

        if (!topics.containsKey(topicId)) {
            System.out.println("Topic not found.");
            return;
        }

        // System.out.print("Enter message (max 100 characters): ");
        // String message = consoleReader.readLine();
        if (message.length() > MAX_MESSAGE_LENGTH) {
            System.out.println("Message too long. Truncating to 100 characters.");
            message = message.substring(0, MAX_MESSAGE_LENGTH);
        }

        out.println("PUBLISH:" + topicId + ":" + message + ":" + name);
        synchronized(socket){
            String response = in.readLine();
            System.out.println("Received response : " + response);
            if (response.startsWith("SUCCESS:")) {
                System.out.println("Message published to topic: " + topics.get(topicId));
            } else {
                System.out.println("Failed to publish message: " + response);
            }
        }
    }

    private void showSubscriberCount() throws IOException {
        if (topics.isEmpty()) {
            System.out.println("No topics available.");
            return;
        }

        for (Map.Entry<UUID, String> entry : topics.entrySet()) {
            out.println("GETSUBSCRIBERCOUNT:" + entry.getKey());
            String response = in.readLine();
            String[] parts = response.split(":");
            if (parts.length == 2 && parts[0].equals("SUBSCRIBERCOUNT")) {
                System.out.println("Topic: " + entry.getValue() + " (ID: " + entry.getKey() + ") - Subscribers: " + parts[1]);
            } else {
                System.out.println("Error getting subscriber count for topic: " + entry.getValue());
            }
        }
    }

    private void deleteTopic() throws IOException {
        if (topics.isEmpty()) {
            System.out.println("No topics available to delete.");
            return;
        }

        System.out.println("Available topics:");
        for (Map.Entry<UUID, String> entry : topics.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        System.out.print("Enter topic ID to delete: ");
        String topicIdStr = consoleReader.readLine();
        UUID topicId;
        try {
            topicId = UUID.fromString(topicIdStr);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid topic ID.");
            return;
        }

        if (!topics.containsKey(topicId)) {
            System.out.println("Topic not found.");
            return;
        }

        out.println("DELETETOPIC:" + topicId + ":" + name);
        String response = in.readLine();
        if (response.startsWith("SUCCESS:")) {
            topics.remove(topicId);
            System.out.println("Topic deleted: " + topicId);
        } else {
            System.out.println("Failed to delete topic: " + response);
        }
    }

    private void disconnect() {
        out.println("EXIT:PUBLISHER");
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.out.println("Error while disconnecting: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Publisher <name>");
            System.exit(1);
        }

        String name = args[0];

        new Publisher(name).start();
    }
}









