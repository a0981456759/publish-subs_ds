import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DirectoryService {
    private static final int PORT = 6000;
    private final Map<Integer, BrokerInfo> brokerRegistry = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private static class BrokerInfo {
        String address;
        int port;
        AtomicInteger connectionCount;

        BrokerInfo(String address, int port) {
            this.address = address;
            this.port = port;
            this.connectionCount = new AtomicInteger(0);
        }

        @Override
        public String toString() {
            return address + ":" + port;
        }
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Directory Service is running on port " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("Error starting directory service: " + e.getMessage());
        } finally {
            executorService.shutdown();
        }
    }

    private class ClientHandler implements Runnable {
        private final Socket socket;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String message = in.readLine();
                System.out.println("Received message: " + message);
                
                if (message.startsWith("REGISTER:")) {
                    handleBrokerRegistration(message, out);
                } else if (message.equals("QUERY_BROKERS")) {
                    handleClientQuery(out);
                } else if (message.startsWith("CLIENT_DISCONNECTED:")) {
                    handleClientDisconnection(message);
                } else {
                    out.println("ERROR:Invalid command");
                }
            } catch (IOException e) {
                System.out.println("Error handling client: " + e.getMessage());
            }
        }

        private void handleBrokerRegistration(String message, PrintWriter out) {
            String[] parts = message.split(":");
            if (parts.length != 4) {
                out.println("ERROR:Invalid registration format");
                return;
            }
            int brokerId = Integer.parseInt(parts[1]);
            String address = parts[2];
            int port = Integer.parseInt(parts[3]);
            registerBroker(brokerId, address, port);
            out.println("SUCCESS:Broker registered");
            System.out.println("Broker registered: " + brokerId + " at " + address + ":" + port);
        }

        private void handleClientQuery(PrintWriter out) {
            BrokerInfo leastLoaded = getLeastLoadedBroker();
            if (leastLoaded != null) {
                leastLoaded.connectionCount.incrementAndGet();
                out.println(leastLoaded.toString());
                System.out.println("Assigned client to broker: " + leastLoaded);
            } else {
                out.println("ERROR:No brokers available");
                System.out.println("No brokers available for client query");
            }
        }

        private void handleClientDisconnection(String message) {
            String[] parts = message.split(":");
            if (parts.length != 2) {
                System.out.println("ERROR:Invalid disconnection message format");
                return;
            }
            int brokerId = Integer.parseInt(parts[1]);
            clientDisconnected(brokerId);
            System.out.println("Client disconnected from broker: " + brokerId);
        }
    }

    private void registerBroker(int brokerId, String address, int port) {
        brokerRegistry.put(brokerId, new BrokerInfo(address, port));
    }

    private BrokerInfo getLeastLoadedBroker() {
        return brokerRegistry.values().stream()
            .min(Comparator.comparingInt(b -> b.connectionCount.get()))
            .orElse(null);
    }

    private void clientDisconnected(int brokerId) {
        BrokerInfo broker = brokerRegistry.get(brokerId);
        if (broker != null) {
            broker.connectionCount.decrementAndGet();
        }
    }

    public static void main(String[] args) {
        new DirectoryService().start();
    }
}