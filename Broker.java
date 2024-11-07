import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Broker {
    // Constrants for broker conf
    private static final int[] BROKER_PORTS = {5003, 5001, 5002};
    private static final int MAX_PUBLISHERS = 5;
    private static final int MAX_SUBSCRIBERS = 10;
    private static final int CONNECTION_TIMEOUT_MS = 5000;
    private static final int MAX_MESSAGE_LENGTH = 100;
    private static final String DIRECTORY_SERVICE_HOST = "localhost";
    private static final int DIRECTORY_SERVICE_PORT = 6000;
    
    // Broker attributes
    private final int brokerId;
    private final int port;
    private final Map<Integer, String> brokerAddresses = new ConcurrentHashMap<>();
    private final Map<UUID, Topic> topics = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerHandler> brokerHandlers = new ConcurrentHashMap<>();
    private final Set<String> connectedPublishers = ConcurrentHashMap.newKeySet();
    private final Set<String> connectedSubscribers = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService clientHandlerExecutor = Executors.newCachedThreadPool();

    // Amount of publishers and subscribers
    public int publisherAmount = 0;
    public int subscriberAmount = 0;

    // Topic attributes
    private class Topic {
        String name;
        String publisherName;
        Set<ClientHandler> subscribers = ConcurrentHashMap.newKeySet();
        Set<String> onlineSubscribers = ConcurrentHashMap.newKeySet();

        Topic(String name, String publisherName) {
            this.name = name;
            this.publisherName = publisherName;
        }
    }

    // Broker constructor
    public Broker(int brokerId) {
        this.brokerId = brokerId;
        this.port = BROKER_PORTS[brokerId];
        for (int i = 0; i < BROKER_PORTS.length; i++) {
            if (i != brokerId) {
                brokerAddresses.put(i, "localhost:" + BROKER_PORTS[i]);
            }
        }
        registerWithDirectoryService();
        
    }

    // Register broker with directory service
    private void registerWithDirectoryService() {
        try (Socket socket = new Socket(DIRECTORY_SERVICE_HOST, DIRECTORY_SERVICE_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            // Register broker with directory service
            String registrationMessage = "REGISTER:" + brokerId + ":localhost:" + port;
            System.out.println("Registering with directory service: " + registrationMessage);
            out.println(registrationMessage);

            String response = in.readLine();
            if ("SUCCESS".equals(response)) {
                System.out.println("Successfully registered with directory service.");
            } else {
                // System.out.println("Failed to register with directory service: " + response);
            }
        } catch (IOException e) {
            System.out.println("Error connecting to directory service: " + e.getMessage());
        }
    }

    // Start broker
    public void start() {
        new Thread(this::startServer).start();
        // Connect to other brokers (5sec)
        scheduler.scheduleWithFixedDelay(this::connectToOtherBrokers, 0, 5, TimeUnit.SECONDS);
    }

    // Start server
    private void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Broker " + brokerId + " is running on port " + port);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientHandlerExecutor.submit(new ClientHandler(clientSocket));
                } catch (IOException e) {
                    System.out.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Could not start server: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    // Connect to other brokers
    private void connectToOtherBrokers() {
        for (Map.Entry<Integer, String> entry : brokerAddresses.entrySet()) {
            int otherBrokerId = entry.getKey();
            if (!brokerHandlers.containsKey(otherBrokerId)) {
                String[] hostPort = entry.getValue().split(":");
                String host = hostPort[0];
                int port = Integer.parseInt(hostPort[1]);
                tryConnectToBroker(otherBrokerId, host, port);
            }
        }
    }

    // Try to connect to other brokers
    private void tryConnectToBroker(int otherBrokerId, String host, int port) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_MS);
            BrokerHandler handler = new BrokerHandler(socket, otherBrokerId);
            brokerHandlers.put(otherBrokerId, handler);
            handler.start();
            System.out.println("Connected to Broker " + otherBrokerId);
            // Broadcast amount of publishers and subscribers to other brokers
            broadcastToOtherBrokers("AMOUNT:"+publisherAmount +":"+ subscriberAmount);
        } catch (IOException e) {
            System.out.println("Failed to connect to Broker " + otherBrokerId + ". Will retry later.");
        }
    }

    private class ClientHandler implements Runnable {
        private final Socket socket;
        private PrintWriter out;
        private BufferedReader in;
        private String clientName;
        private boolean isPublisher;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    handleClientMessage(inputLine);
                }
            } catch (IOException e) {
                System.out.println("Error handling client: " + e.getMessage());
            } finally {
                cleanupConnection();
            }
        }

        private void printConnectedPublishers(){
            System.out.println("Publishers members:");
            for (String publisher : connectedPublishers){
                System.out.println(publisher);
            }
        }

        private void handlePublisherConnection(boolean isBroadcast) {
            synchronized (connectedPublishers) {
                System.out.println("before_Connect_AMOUNTTTTT: " + publisherAmount);
                System.out.println("before_CONNECT_MAP_AMOUNT: " + connectedPublishers.size());
                System.out.println("before_CONNECT_MAP_members: ");
                printConnectedPublishers();
                if (isBroadcast){
                    publisherAmount +=1;
                    connectedPublishers.add(clientName);
                    System.out.println("isBroadcast_Connect_AMOUNTTTTT: " + publisherAmount);
                    System.out.println("isBroadcast_CONNECT_MAP_AMOUNT: " + connectedPublishers.size());
                    System.out.println("isBroadcast_CONNECT_MAP_members: ");
                    printConnectedPublishers();
                    return;
                }
                if (connectedPublishers.size() >= MAX_PUBLISHERS) {
                    sendError("Max publishers reached");
                    System.out.println("Amount of publishers: " + publisherAmount);
                    return;
                }
                if (connectedPublishers.contains(clientName)) {;
                    sendError("Publisher name already in use");
                    return;
                }
                isPublisher = true;
                connectedPublishers.add(clientName);
                System.out.println("Publisher connected: " + clientName);
                publisherAmount +=1;
                System.out.println("after_Connect_AMOUNTTTTT: " + publisherAmount);
                System.out.println("after_CONNECT_MAP_AMOUNT: " + connectedPublishers.size());
                System.out.println("after_CONNECT_MAP_members: ");
                printConnectedPublishers();
                broadcastToOtherBrokers("PUBLISHER:" +clientName);
                
                
                sendSuccess("Connected as publisher");
                // broadcastToOtherBrokers("PUBLISHER:" + clientName);
            }
            
        }

        private void handleSubscriberConnection(boolean isBroadcast) {
            
            synchronized(connectedSubscribers){
                System.out.println("berofre_Connect_AMOUNTTTTT: " + subscriberAmount);
                System.out.println("before_CONNECT_MAP_AMOUNT: " + connectedSubscribers.size());
                if (isBroadcast){
                    subscriberAmount +=1;
                    connectedSubscribers.add(clientName);
                    return;
                }
                if ( connectedSubscribers.size() >= MAX_SUBSCRIBERS) {
                    sendError("Max subscribers reached");
                    System.out.println("Amount of subscribers: " + subscriberAmount);
                    return;
                }
                if (connectedSubscribers.contains(clientName)) {
                    sendError("Subscriber name already in use");
                    return;
                }
                isPublisher = false;
                connectedSubscribers.add(clientName);
                System.out.println("Subscriber connected: " + clientName);
                subscriberAmount +=1;
                System.out.println("after_Connect_AMOUNTTTTT: " + subscriberAmount);
                System.out.println("after_CONNECT_MAP_AMOUNT: " + connectedSubscribers.size());
                broadcastToOtherBrokers("SUBSCRIBER:" + clientName);
                sendSuccess("Connected as subscriber");
            
            // broadcastToOtherBrokers("SUBSCRIBER:" + clientName);
        }
    }

        private void handleClientMessage(String message) {
            String orginalMessage = message;
            System.out.println("Received message: " + message);
            // String[] parts = message.split(":", 3);
            ArrayList<String> parts = new ArrayList<>();
            for (int i =0;;i++){
                
                if (message.indexOf(":") == -1){
                    parts.add(message);
                    // System.out.println("Message: " + parts.get(i));
                    break;
                }

                parts.add(message.substring(0, message.indexOf(":")));
                // System.out.println("Message: " + parts.get(i));
                message = message.substring(message.indexOf(":") + 1, message.length());
            }
            boolean isBroadcast = false;
            if (parts.get(0).equals("Broadcast")) {
                parts.remove(0);
                isBroadcast = true;
            }
            // System.out.println("Parts sizee: " + parts.size());
            switch (parts.get(0)) {
                case "NEWTOPIC":
                    createTopic(UUID.fromString(parts.get(1)), parts.get(2), isBroadcast, parts);
                    break;
                case "PUBLISHER":
                    clientName = parts.get(1);
                    handlePublisherConnection(isBroadcast);
                    break;
                case "SUBSCRIBER":
                    clientName = parts.get(1);
                    handleSubscriberConnection(isBroadcast);
                    break;
                case "CREATETOPIC":
                    createTopic(UUID.fromString(parts.get(1)), parts.get(2), isBroadcast, parts);
                    break;
                case "PUBLISH":
                    publish(UUID.fromString(parts.get(1)), parts.get(2), isBroadcast, parts);
                    break;
                case "SUBSCRIBE":
                    subscribe(UUID.fromString(parts.get(1)), isBroadcast, parts);
                    break;
                case "UNSUBSCRIBE":
                    unsubscribe(UUID.fromString(parts.get(1)), isBroadcast, parts);
                    break;
                case "GETSUBSCRIBERCOUNT":
                    getSubscriberCount(UUID.fromString(parts.get(1)), isBroadcast);
                    break;
                case "DELETETOPIC":
                    deleteTopic(UUID.fromString(parts.get(1)), isBroadcast,parts);
                    break;
                case "LISTTOPICS":
                    listTopics();
                    break;
                case "AMOUNT":
                    publisherAmount = Integer.parseInt(parts.get(1));
                    subscriberAmount = Integer.parseInt(parts.get(2));
                    break;
                case "EXIT":
                    System.out.println("Client disconnected: " + clientName);
                    System.out.println("isBroadcast: " + isBroadcast);
                    System.out.println("parts: " + parts.get(1));
                    cleanupConnection();
                    if (parts.get(1).equals("PUBLISHER")){
                        System.out.println("Amount of publishers: " + publisherAmount);
                        publisherAmount -=1;
                        System.out.println("Amount of publishers-1: " + publisherAmount);
                        if (!isBroadcast){
                            broadcastToOtherBrokers("EXIT:PUBLISHER:"+ publisherAmount);
                        }
                        
                    }
                    else if (parts.get(1).equals("SUBSCRIBER")){
                        
                        System.out.println("Amount of subscribers: " + subscriberAmount);
                        subscriberAmount -=1;
                        System.out.println("Amount of subscribers-1: " + subscriberAmount);
                        if (!isBroadcast){
                            broadcastToOtherBrokers("EXIT:SUBSCRIBER:"+ subscriberAmount);
                        }
                    }
                    break;
                case "REMOVE":
                    if (parts.get(1).equals("PUBLISHER")){
                        connectedPublishers.remove(parts.get(2));
                    }else if (parts.get(1).equals("SUBSCRIBER")){
                        connectedSubscribers.remove(parts.get(2));
                    }
                    break;
                default:
                    sendError("Invalid command");
            }
        }

        private void createTopic(UUID topicId, String topicName, boolean isBroadcast, ArrayList<String> parts) {
            if(isBroadcast){
                if (topics.containsKey(topicId)) {
                    sendError("Topic ID already exists");
                    return;
                }
                topics.put(topicId, new Topic(topicName, parts.get(3)));
            }
                
            
            if(!isBroadcast){
                if (topics.containsKey(topicId)) {
                    sendError("Topic ID already exists");
                    return;
                }
                topics.put(topicId, new Topic(topicName, clientName));
                broadcastToOtherBrokers("NEWTOPIC:" + topicId + ":" + topicName + ":" + clientName);
            }
                
            System.out.println("New topic created: " + topicName + " (ID: " + topicId + ")");
            sendSuccess("Topic created");
        }

        private void publish(UUID topicId, String content, boolean isBroadcast, ArrayList<String> parts) {
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendError("Topic not found");
                return;
            }
            if (!topic.publisherName.equals(parts.get(3))) {
                System.out.println(topic.publisherName+" "+parts.get(3));
                sendError("Not authorized to publish to this topic");
                return;
            }
            if (content.length() > MAX_MESSAGE_LENGTH) {
                sendError("Message too long (max " + MAX_MESSAGE_LENGTH + " characters)");
                return;
            }
            String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
            String formattedMessage = String.format("%s %s:%s: %s", timestamp, topicId, topic.name, content);
            for (ClientHandler subscriber : topic.subscribers) {
                subscriber.sendMessage(formattedMessage);
            }
            if(!isBroadcast)
                broadcastToOtherBrokers("PUBLISH:" + topicId + ":" + content+ ":" + clientName);
            System.out.println("Message published to topic: " + topic.name + " (ID: " + topicId + ")");
            sendSuccess("Message published");
        }

        private void subscribe(UUID topicId, boolean isBroadcast, ArrayList<String> parts) {
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendError("Topic not found");
                return;
            }
            System.out.println(this.clientName+" "+port);
            System.out.println("Client " + clientName +" subscribed to topic: " + topic.name + " (ID: " + topicId + ")");
            if(!isBroadcast) {
                topic.onlineSubscribers.add(this.clientName+" "+port);
                topic.subscribers.add(this);
                broadcastToOtherBrokers("SUBSCRIBE:" + topicId + ":" + this.clientName + ":" + port);
            }else {
                topic.onlineSubscribers.add(parts.get(2)+" "+parts.get(3));
            }
            sendSuccess("SUBSCRIBED:" + topicId + ":" + topic.name+ ":" + topic.publisherName);

        }

        private void unsubscribe(UUID topicId, boolean isBroadcast, ArrayList<String> parts) {
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendError("Topic not found");
                return;
            }
            topic.subscribers.remove(this);
            System.out.println("Client " + clientName + " unsubscribed from topic: " + topic.name + " (ID: " + topicId + ")");
            if(!isBroadcast) {
                topic.onlineSubscribers.remove(this.clientName+" "+port);
                topic.subscribers.remove(this);
                broadcastToOtherBrokers("UNSUBSCRIBE:" + topicId + ":" + this.clientName + ":" + port);
            }else {
                topic.onlineSubscribers.remove(parts.get(2)+" "+parts.get(3));
            }
            sendSuccess("UNSUBSCRIBED:"+topicId + ":" + topic.name+ ":" + topic.publisherName);
        }

        private void getSubscriberCount(UUID topicId, boolean isBroadcast) {
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendError("Topic not found");
                return;
            }
            sendMessage("SUBSCRIBERCOUNT:" + topic.onlineSubscribers.size());

        }

        private void deleteTopic(UUID topicId, boolean isBroadcast, ArrayList<String> parts) {
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendError("Topic not found");
                return;
            }
            if (!topic.publisherName.equals(parts.get(2))) {
                sendError("Not authorized to delete this topic");
                return;
            }
            for (ClientHandler subscriber : topic.subscribers) {
                subscriber.sendMessage("TOPICDELETED:" + topicId + ":" + topic.name);
            }
            topics.remove(topicId);
            if(!isBroadcast)
                broadcastToOtherBrokers("DELETETOPIC:" + topicId + ":" + clientName);
            System.out.println("Topic deleted: " + topic.name + " (ID: " + topicId + ")");
            sendSuccess("Topic deleted");
        }

        private void listTopics() {
            if (topics.isEmpty()) {
                sendMessage("TOPICLIST:EMPTY");
            } else {
                StringBuilder topicList = new StringBuilder("TOPICLIST:");
                for (Map.Entry<UUID, Topic> entry : topics.entrySet()) {
                    topicList.append(entry.getKey()).append("|")
                             .append(entry.getValue().name).append("|")
                             .append(entry.getValue().publisherName).append(",");
                }
                topicList.setLength(topicList.length() - 1);
                sendMessage(topicList.toString());
            }
        }

        private void sendMessage(String message) {
            out.println(message);
            out.flush();
        }

        private void sendSuccess(String message) {
            String successMessage = "SUCCESS:" + message;
            System.out.println("Sending success response: " + successMessage);
            sendMessage(successMessage);
        }
        
        private void sendError(String message) {
            String errorMessage = "ERROR:" + message;
            System.out.println("Sending error response: " + errorMessage);
            sendMessage(errorMessage);
        }

        private void cleanupConnection() {
            try {
                if (socket != null) socket.close();
                if (isPublisher) {
                    connectedPublishers.remove(clientName);
                    System.out.println("DISCONNECT_MAP_AMOUNT: " + connectedPublishers.size());
                    System.out.println("Publisher disconnected: " + clientName);
                    broadcastToOtherBrokers("REMOVE:PUBLISHER:" + clientName);
        
                    Iterator<Map.Entry<UUID, Topic>> iterator = topics.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<UUID, Topic> entry = iterator.next();
                        Topic topic = entry.getValue();
                        if (topic.publisherName.equals(clientName)) {
                            deleteTopic(entry.getKey(), false, new ArrayList<>(Arrays.asList("DELETETOPIC", entry.getKey().toString(), clientName)));
                        }
                    }
                } else {
                    connectedSubscribers.remove(clientName);
                    System.out.println("DISCONNECT_MAP_AMOUNT: " + connectedSubscribers.size());
                    System.out.println("Subscriber disconnected: " + clientName);
                    broadcastToOtherBrokers("REMOVE:SUBSCRIBER:" + clientName);
                }
        
                for (Topic topic : topics.values()) {
                    topic.subscribers.remove(this);
                }
            } catch (IOException e) {
                System.out.println("Error closing client connection: " + e.getMessage());
            }
        }
    }

    private class BrokerHandler extends Thread {
        private final Socket socket;
        private final int otherBrokerId;
        private PrintWriter out;
        private BufferedReader in;

        public BrokerHandler(Socket socket, int otherBrokerId) {
            this.socket = socket;
            this.otherBrokerId = otherBrokerId;
        }

        @Override
        public void run() {
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    handleBrokerMessage(inputLine);
                }
            } catch (IOException e) {
                System.out.println("Connection lost with Broker " + otherBrokerId);
            } finally {
                cleanupBrokerConnection();
            }
        }

        private void handleBrokerMessage(String message) {
            String[] parts = message.split(":", 4);
            if (parts.length < 2) {
                System.out.println("Invalid message format from broker: " + message);
                return;
            }

            String command = parts[0];
            switch (command) {
                case "NEWTOPIC":
                    handleNewTopic(parts);
                    break;
                case "DELETETOPIC":
                    handleDeleteTopic(parts);
                    break;
                case "PUBLISH":
                    handlePublish(parts);
                    break;
                default:
                    System.out.println("Unknown command received from broker: " + command);
            }
        }

        private void handleNewTopic(String[] parts) {
            if (parts.length < 4) {
                System.out.println("Invalid NEWTOPIC message format");
                return;
            }
            try {
                UUID topicId = UUID.fromString(parts[1]);
                String topicName = parts[2];
                String publisherName = parts[3];
                if (!topics.containsKey(topicId)) {
                    topics.put(topicId, new Topic(topicName, publisherName));
                    System.out.println("New topic added from another broker: " + topicName + " (ID: " + topicId + ", Publisher: " + publisherName + ")");
                } else {
                    System.out.println("Topic already exists: " + topicName + " (ID: " + topicId + ")");
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid topic ID in NEWTOPIC message: " + parts[1]);
            }
        }

        private void handleDeleteTopic(String[] parts) {
            if (parts.length < 2) {
                System.out.println("Invalid DELETETOPIC message format");
                return;
            }
            try {
                UUID topicId = UUID.fromString(parts[1]);
                Topic removedTopic = topics.remove(topicId);
                if (removedTopic != null) {
                    System.out.println("Topic removed: " + removedTopic.name + " (ID: " + topicId + ")");
                    // Notify subscribers about topic deletion
                    for (ClientHandler subscriber : removedTopic.subscribers) {
                        subscriber.sendMessage("TOPICDELETED:" + topicId + ":" + removedTopic.name);
                    }
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid topic ID in DELETETOPIC message: " + parts[1]);
            }
        }

        private void handlePublish(String[] parts) {
            if (parts.length < 3) {
                System.out.println("Invalid PUBLISH message format");
                return;
            }
            try {
                UUID topicId = UUID.fromString(parts[1]);
                String content = parts[2];
                Topic topic = topics.get(topicId);
                if (topic != null) {
                    for (ClientHandler subscriber : topic.subscribers) {
                        subscriber.sendMessage("MESSAGE:" + topicId + ":" + topic.name + ":" + content);
                    }
                    System.out.println("Message from another broker published to topic: " + topic.name + " (ID: " + topicId + ")");
                } else {
                    System.out.println("Received message for non-existent topic: " + topicId);
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid topic ID in PUBLISH message: " + parts[1]);
            }
        }

        public void sendMessage(String message) {
            out.println(message);
            out.flush();
        }

        private void cleanupBrokerConnection() {
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                System.out.println("Error closing broker connection: " + e.getMessage());
            }
            brokerHandlers.remove(otherBrokerId);
        }
    }

    private void broadcastToOtherBrokers(String message) {
        System.out.println("Broker!! " + brokerId + " broadcasting to other brokers!!!: " + message);
        System.out.println("Broadcasting to other brokers: " + message);
        for (BrokerHandler handler : brokerHandlers.values()) {
            try {
                handler.sendMessage("Broadcast:" + message);
            } catch (Exception e) {
                System.out.println("Failed to send message to broker: " + e.getMessage());
            }
        }
    }
    private void shutdown() {
        System.out.println("Shutting down Broker " + brokerId);
        scheduler.shutdownNow();
        clientHandlerExecutor.shutdownNow();
        for (BrokerHandler handler : brokerHandlers.values()) {
            try {
                handler.socket.close();
            } catch (IOException e) {
                System.out.println("Error closing broker connection: " + e.getMessage());
            }
        }
        brokerHandlers.clear();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Broker <brokerId>");
            System.exit(1);
        }

        int brokerId = Integer.parseInt(args[0]);
        if (brokerId < 0 || brokerId >= BROKER_PORTS.length) {
            System.out.println("Invalid brokerId. Must be between 0 and " + (BROKER_PORTS.length - 1));
            System.exit(1);
        }

        new Broker(brokerId).start();
    }
}