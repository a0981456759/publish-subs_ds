import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Subscriber {
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final int RETRY_DELAY_MS = 5000;
    private static final int CONNECTION_TIMEOUT_MS = 5000;

    private final String name;
    private String host;
    private int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final Map<UUID, TopicInfo> subscriptions = new ConcurrentHashMap<>();
    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    private final BufferedReader consoleReader;

    private static class TopicInfo {
        String name;
        String publisherName;

        TopicInfo(String name, String publisherName) {
            this.name = name;
            this.publisherName = publisherName;
        }
    }

    public Subscriber(String name) {
        this.name = name;
        this.consoleReader = new BufferedReader(new InputStreamReader(System.in));
    }

    public void start() {
        try {
            queryDirectoryService();
            connectToBroker();
            new Thread(this::receiveMessages).start();
            new Thread(this::displayMessages).start();
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
                System.out.println("Attempting to connect to broker at " + host + ":" + port + " (Attempt " + (attempts + 1) + ")");
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_MS);
                System.out.println("Socket connected successfully");
                
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                System.out.println("Sending init message: SUBSCRIBER:" + name);
                out.println("SUBSCRIBER:" + name);
                
                String response = waitForResponse();
                System.out.println("Received response from broker: " + response);
                
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
                System.out.println("Connection attempt failed: " + e.getMessage());
                attempts++;
                if (attempts >= MAX_RETRY_ATTEMPTS) {
                    throw new IOException("Failed to connect after " + MAX_RETRY_ATTEMPTS + " attempts: " + e.getMessage());
                }
                System.out.println("Retrying in " + (RETRY_DELAY_MS / 1000) + " seconds...");
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
        System.out.println("Waiting for response from broker...");
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < CONNECTION_TIMEOUT_MS) {
            if (in.ready()) {
                String response = in.readLine();
                System.out.println("Response received: " + response);
                return response;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for response", e);
            }
        }
        System.out.println("Timeout while waiting for response");
        return null; // Timeout occurred
    }

    private void receiveMessages() {
        try {
            System.out.println("Message receiving thread started");
            String message;
            while ((message = in.readLine()) != null) {
                System.out.println("Received message: " + message);
                messageQueue.put(message);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
    }

    private void displayMessages() {
        try {
            while (true) {
                String message = messageQueue.take();
                handleReceivedMessage(message);
            }
        } catch (InterruptedException e) {
            System.out.println("Message display interrupted: " + e.getMessage());
        }
    }

    private void handleReceivedMessage(String message) {
        String[] parts = message.split(":", 2);
        if (parts.length < 2) return;

        String messageType = parts[0];
        String content = parts[1];

        switch (messageType) {
            case "MESSAGE":
                handleIncomingMessage(content);
                break;
            case "TOPICDELETED":
                handleTopicDeleted(content);
                break;
            case "TOPICLIST":
                handleTopicList(content);
                break;
            case "SUBSCRIBERCOUNT":
                handleSubscriberCount(content);
                break;
            case "SUCCESS":
                handleSuccessMessage(content);
                break;
            case "ERROR":
                handleErrorMessage(content);
                break;
            default:
                // System.out.println("Received unknown message type: " + messageType);
                break;
        }
    }

    private void handleIncomingMessage(String content) {
        String[] parts = content.split(":", 2);
        if (parts.length == 2) {
            UUID topicId = UUID.fromString(parts[0]);
            String messageContent = parts[1];
            TopicInfo topic = subscriptions.get(topicId);
            if (topic != null) {
                System.out.println("\nReceived message:");
                System.out.println("Topic: " + topic.name + " (ID: " + topicId + ")");
                System.out.println("Publisher: " + topic.publisherName);
                System.out.println("Content: " + messageContent);
            }
        }
    }

    private void handleTopicDeleted(String content) {
        String[] parts = content.split(":", 2);
        if (parts.length == 2) {
            UUID topicId = UUID.fromString(parts[0]);
            String topicName = parts[1];
            TopicInfo deletedTopic = subscriptions.remove(topicId);
            if (deletedTopic != null) {
                System.out.println("\nNotification: Topic '" + topicName + "' (ID: " + topicId + ") has been deleted by the publisher.");
            }
        }
    }

    private void handleTopicList(String content) {
        if (content.equals("EMPTY")) {
            System.out.println("No topics available.");
        } else {
            String[] topics = content.split(",");
            System.out.println("\nAvailable Topics:");
            for (String topic : topics) {
                String[] parts = topic.split("\\|");
                if (parts.length == 3) {
                    System.out.println("ID: " + parts[0] + ", Name: " + parts[1] + ", Publisher: " + parts[2]);
                }
            }
        }
    }

    private void handleSubscriberCount(String content) {
        String[] parts = content.split(":", 2);
        if (parts.length == 2) {
            System.out.println("Subscriber count for topic " + parts[0] + ": " + parts[1]);
        }
    }

    private void handleSuccessMessage(String content) {
        System.out.println("Operation successful: " + content);
        String[] parts = content.split(":", 4);
        if (parts.length == 4 && parts[0].equals("SUBSCRIBED")) {
            UUID topicId = UUID.fromString(parts[1]);
            String topicName = parts[2];
            String publisherName = parts[3];
            subscriptions.put(topicId, new TopicInfo(topicName, publisherName));
            System.out.println("Successfully subscribed to topic: " + topicName + " (ID: " + topicId + ")");
        }else if (parts.length == 4 && parts[0].equals("UNSUBSCRIBED")) {
            UUID topicId = UUID.fromString(parts[1]);
            String topicName = parts[2];
            String publisherName = parts[3];
            subscriptions.remove(topicId);
            System.out.println("Successfully unsubscribed from topic: " + topicName + " (ID: " + topicId + ")");
        }
    }

    private void handleErrorMessage(String content) {
        System.out.println("Error: " + content);
        if (content.startsWith("SUBSCRIBE:")) {
            System.out.println("Failed to subscribe to topic. Please try again.");
        }else if (content.equals("Topic not found")) {
            System.out.println("Topic not found. Please try again.");
        }
    }

    private void runConsoleMenu() throws IOException {
        while (true) {
            // printMenu();
            System.out.println("Please select command: list, sub, current, unsub, exit.");
            // System.out.println("1. List All Available Topics");
            // System.out.println("2. Subscribe to a Topic");
            // System.out.println("3. Show Current Subscriptions");
            // System.out.println("4. Unsubscribe from a Topic");
            // System.out.println("5. Exit");
            // System.out.print("Enter your choice: ");

            String choice = consoleReader.readLine();
            String[] parts = choice.split(" ",2);
            String command = parts[0];
            switch (command) {
                case "list":
                    listAllTopics();
                    break;
                case "sub":
                    if (parts.length < 2) {
                        System.out.println("Please provide a topic ID to subscribe.");
                    } else {
                        try {
                            UUID topicId = UUID.fromString(parts[1]);
                            subscribeToTopic(topicId);
                        } catch (IllegalArgumentException e) {
                            System.out.println("Invalid topic ID format. Please enter a valid UUID.");
                        }
                    }
                    break;
                case "current":
                    showCurrentSubscriptions();
                    // printMenu();
                    break;
                case "unsub":
                    if(parts.length<2){
                        System.out.println("Invalid command. Please try again.");
                        break;
                    }else {
                        try {
                            UUID topicId = UUID.fromString(parts[1]);
                            unsubscribeFromTopic(topicId);
                        } catch (IllegalArgumentException e) {
                            System.out.println("Invalid topic ID format. Please enter a valid UUID.");
                        }
                    }
                    break;
                case "exit":
                    disconnect();
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }

            
            System.out.println("Press Enter to continue...");
            consoleReader.readLine();
        }
    }
    // private void printMenu() {
    //     System.out.println("\nPlease select command: list, sub, current, unsub, exit.");
    // }
    private void listAllTopics() throws IOException {
        System.out.println("Requesting topic list from broker...");
        out.println("LISTTOPICS");
        out.flush();

    }

    private void subscribeToTopic(UUID topicId) throws IOException {
        // System.out.print("Enter topic ID to subscribe: ");
        out.println("SUBSCRIBE:" + topicId + ":" + name + ":" + port);
        System.out.println("Subscription request sent for topic ID: " + topicId);
        
    }

    private void showCurrentSubscriptions() {
        if (subscriptions.isEmpty()) {
            System.out.println("You are not subscribed to any topics.");
        } else {
            System.out.println("\nCurrent Subscriptions:");
            for (Map.Entry<UUID, TopicInfo> entry : subscriptions.entrySet()) {
                System.out.println("ID: " + entry.getKey() + ", Name: " + entry.getValue().name + ", Publisher: " + entry.getValue().publisherName);
            }
        }
    }

    private void unsubscribeFromTopic(UUID key) throws IOException {
        String topicId = key.toString();
        if (subscriptions.isEmpty()) {
            System.out.println("You are not subscribed to any topics.");
            return;
        }
        out.println("UNSUBSCRIBE:" + topicId + ":" + name + ":" + port);
    }

    private void disconnect() {
        try {
            for (UUID key : subscriptions.keySet()) {
                unsubscribeFromTopic(key);
            out.println("UNSUBSCRIBE:" + key + ":" + name + ":" + port);
        }}catch (IOException e) {
            System.out.println("Error while disconnecting: " + e.getMessage());
        }
        out.println("EXIT:SUBSCRIBER");
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
            System.out.println("Disconnected from broker.");
        } catch (IOException e) {
            System.out.println("Error while disconnecting: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Subscriber <name>");
            System.exit(1);
        }

        String name = args[0];


        new Subscriber(name).start();
    }
}