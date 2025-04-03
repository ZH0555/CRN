// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Zi Hen (Andy) Lin
//  230019400
//  zi.lin@city.ac.uk

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.SocketTimeoutException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.io.IOException;

// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {
    private String nodeName;
    private byte[] hashID;
    private DatagramSocket socket;
    private final Map<String, String> keyValuePairs = new HashMap<>();
    private final Stack<String> relayStack = new Stack<>();
    private final Map<String, InetSocketAddress> knownNodes = new HashMap<>();
    private final List<String> infoMessages = new ArrayList<>();
    private final Map<String, String> nearestNodeResponses = new HashMap<>();
    private final Set<String> seenTransactions = Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(1000, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > 1000;
        }
    });
    private final Random randomG = new Random();
    private String recentRead = null;
    private boolean recentExistence = false;
    private final boolean debugLogs = false;

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || nodeName.trim().isEmpty() || !nodeName.startsWith("N:")) {
            throw new Exception("Error in node name!");
        }

        this.nodeName = nodeName;
        System.out.println("Node name set to: " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        if (socket != null && !socket.isClosed()) {
            throw new Exception("Port is already open");
        }

        if (portNumber < 20110 || portNumber > 20130) {
            throw new Exception("Port number must be between 20110 and 20130");
        }

        socket = new DatagramSocket(portNumber);
        if (debugLogs) System.out.println("Socket active on port " + portNumber);
    }

    
    private String wrapMessage(String s) {
        long blanks = s.chars().filter(c -> c == ' ').count();
        return blanks + " " + s + " ";
    }

    private String unwrapMessage(String s) {
        int space = s.indexOf(' ');
        return (space != -1 && s.length() > space + 1) ? s.substring(space + 1, s.length() - 1) : null;
    }

    private void msgResponse(InetAddress addr, int port, String msg) {
        if (addr == null || port == -1) {
            throw new IllegalArgumentException("Address or port not set");
        }
        try {
            for (int i = relayStack.size() - 1; i >= 0; i--) {
                msg = "V " + wrapMessage(relayStack.get(i)) + msg;
            }
            byte[] data = msg.getBytes();
            DatagramPacket p = new DatagramPacket(data, data.length, addr, port);
            socket.send(p);
            if (debugLogs) System.out.println("Message: " + msg);
        } catch (IOException ex) {
            if (debugLogs) System.err.println("Failed to send due to: " + ex.getMessage());
        }
    }

    private String[] extractKeyValue(String input) {
        try {
            String[] arr = input.trim().split(" ", 4);
            if (arr.length == 4) return new String[]{arr[1], arr[3]};
        } catch (Exception ignored) {}
        return null;
    }

    private void parseMessage(String msg, InetAddress addr, int port) {
        try {
            if (Math.random() < 0.1) return;

            String[] tokens = msg.trim().split(" ", 3);
            if (tokens.length < 2) return;
            String tx = tokens[0], kind = tokens[1];
            if (seenTransactions.contains(tx)) return;
            seenTransactions.add(tx);

            switch (kind) {
                case "G" -> { 
                    msgResponse(addr, port, tx + " H " + wrapMessage(nodeName));
                }
                case "H" -> {
                    if (tokens.length > 2) {
                        String incoming = unwrapMessage(tokens[2]);
                        if (incoming != null)
                            knownNodes.put(incoming, new InetSocketAddress(addr, port));
                    }
                }
                case "N" -> {
                    if (tokens.length > 2) {
                        String hash = tokens[2].trim();
                        List<String> options = new ArrayList<>(knownNodes.keySet());
                        options.removeIf(x -> !x.startsWith("N:"));
                        options.sort(Comparator.comparingInt(n -> {
                            try {
                                return calcDistance(hash, byteToString(HashID.computeHashID(n)));
                            } catch (Exception e) {
                                return Integer.MAX_VALUE;
                            }
                        }));
                        StringBuilder resp = new StringBuilder(tx + " O");
                        for (int i = 0; i < Math.min(3, options.size()); i++) {
                            String name = options.get(i);
                            InetSocketAddress target = knownNodes.get(name);
                            if (target != null) {
                                String val = target.getAddress().getHostAddress() + ":" + target.getPort();
                                resp.append(" ").append(wrapMessage(name)).append(wrapMessage(val));
                            }
                        }
                        msgResponse(addr, port, resp.toString());
                    }
                }
                case "O" -> {
                    if (tokens.length > 2) nearestNodeResponses.put(tx, tokens[2]);
                }
                case "I" -> {
                    if (tokens.length > 2) {
                        String infoMessage = unwrapMessage(tokens[2]);
                        if (infoMessage != null) {
                            infoMessages.add(infoMessage);
                            if (debugLogs) System.out.println("INFO: " + infoMessage);
                        }
                }
                }
                case "V" -> {
                    if (tokens.length > 2) {
                        String[] inner = tokens[2].split(" ", 2);
                        if (inner.length == 2) {
                            String next = unwrapMessage(inner[0]);
                            String payload = inner[1];
                            if (next != null) {
                                if (next.equals(nodeName)) {
                                    parseMessage(payload, addr, port);
                                } else if (knownNodes.containsKey(next)) {
                                    InetSocketAddress forwardTo = knownNodes.get(next);
                                    String wrapped = "V " + wrapMessage(next) + payload;
                                    msgResponse(forwardTo.getAddress(), forwardTo.getPort(), wrapped);
                                }
                            }
                        }
                    }
                }
                case "E" -> {
                    String k = unwrapMessage(tokens.length > 2 ? tokens[2] : "");
                    boolean exist = k != null && keyValuePairs.containsKey(k);
                    msgResponse(addr, port, tx + " F " + (exist ? "Y" : "N"));
                }
                case "F" -> {
                    if (tokens.length > 2 && tokens[2].trim().equals("Y")) recentExistence = true;
                }
                case "R" -> {
                    String query = unwrapMessage(tokens.length > 2 ? tokens[2] : "");
                    if (query != null) {
                        String reply = keyValuePairs.containsKey(query) ? keyValuePairs.get(query) : null;
                        msgResponse(addr, port, tx + (reply != null ? " S Y " + wrapMessage(reply) : " S N "));
                    }
                }
                case "S" -> {
                    if (tokens.length > 2) {
                        String[] parts = tokens[2].split(" ", 2);
                        if (parts[0].equals("Y") && parts.length > 1)
                            recentRead = unwrapMessage(parts[1]);
                    }
                }
                case "W" -> {
                    String[] kv = extractKeyValue(tokens.length > 2 ? tokens[2] : "");
                    if (kv != null && kv[0] != null && kv[1] != null) {
                        keyValuePairs.put(kv[0], kv[1]);
                        if (kv[0].startsWith("N:")) {
                            try {
                                String[] ipInfo = kv[1].split(":");
                                if (ipInfo.length == 2)
                                    knownNodes.put(kv[0], new InetSocketAddress(ipInfo[0], Integer.parseInt(ipInfo[1])));
                            } catch (Exception ignored) {}
                        }
                        msgResponse(addr, port, tx + " X A");
                    }
                }
                case "X" -> {
                    if (tokens.length > 2) {
                        String responseChar = tokens[2].trim();
                    } 
                }
        }
        } catch (Exception e) {
            System.err.println("Error message: " + e.getMessage());
        }
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        socket.setSoTimeout(100);
        byte[] packetBuffer = new byte[2048];
        long tStart = System.currentTimeMillis();
        while (delay == 0 || System.currentTimeMillis() - tStart < delay) {
            DatagramPacket datagram = new DatagramPacket(packetBuffer, packetBuffer.length);
            try {
                socket.receive(datagram);
                String received = new String(datagram.getData(), 0, datagram.getLength());
                if (debugLogs) System.out.println("[INFO] " + received);
                parseMessage(received, datagram.getAddress(), datagram.getPort());
            } catch (SocketTimeoutException ignored) {}
        }
    }
    
    @Override
    public boolean isActive(String nodeName) throws Exception {
        return knownNodes.containsKey(nodeName);
    }
    
    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String removeNode = relayStack.pop();
            System.out.println("Removed node: " + removeNode + " from relay stack");
        } else {
            System.out.println("Relay stack is empty.");
        }
    }

    private String attemptLookup(String key, boolean checkExists) throws Exception {
        if (keyValuePairs.containsKey(key)) return keyValuePairs.get(key);

        String hash = byteToString(HashID.computeHashID(key));
        Set<String> querySet = new HashSet<>();
        Queue<String> queue = new LinkedList<>(knownNodes.keySet());

        if (knownNodes.isEmpty()) {
            knownNodes.put("N:azure", new InetSocketAddress("10.216.34.197", 20114));
            queue.add("N:azure");
        }

        while (!queue.isEmpty()) {
            String node = queue.poll();
            if (querySet.contains(node) || !knownNodes.containsKey(node)) continue;
            querySet.add(node);

            InetSocketAddress target = knownNodes.get(node);
            String txID = newTxn();
            if (checkExists) {
                recentExistence = false;
                msgResponse(target.getAddress(), target.getPort(), txID + " E " + wrapMessage(key));
            } else {
                recentRead = null;
                msgResponse(target.getAddress(), target.getPort(), txID + " R " + wrapMessage(key));
            }

            long waitStart = System.currentTimeMillis();
            while (System.currentTimeMillis() - waitStart < 1000) {
                handleIncomingMessages(100);
                if ((checkExists && recentExistence) || (!checkExists && recentRead != null)) {
                    return checkExists ? "YES" : recentRead;
                }
            }

            String nTx = newTxn();
            msgResponse(target.getAddress(), target.getPort(), nTx + " N " + hash);

            long wait2 = System.currentTimeMillis();
            while (!nearestNodeResponses.containsKey(nTx) && System.currentTimeMillis() - wait2 < 1000) {
                handleIncomingMessages(100);
            }

            String raw = nearestNodeResponses.get(nTx);
            if (raw == null) continue;

            String[] lines = raw.trim().split(" ");
            for (int i = 0; i + 3 < lines.length; i += 4) {
                String nodeKey = lines[i + 1];
                String address = lines[i + 3];
                if (nodeKey.startsWith("N:") && address.contains(":")) {
                    String[] info = address.split(":");
                    InetSocketAddress peer = new InetSocketAddress(info[0], Integer.parseInt(info[1]));
                    knownNodes.put(nodeKey, peer);
                    if (!querySet.contains(nodeKey)) queue.add(nodeKey);
                }
            }
        }
        return null;
    }

    @Override
    public boolean exists(String key) throws Exception {
        System.out.println("Checking if the key " + key + " exists..");
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("Error in key!");
        }
        return attemptLookup(key, true) != null;
    }
    
    @Override
    public String read(String key) throws Exception {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("Error in key!");
        }
        return attemptLookup(key, false);
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        if (key == null || value == null || key.trim().isEmpty() || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Error in key or value!");
        }
        keyValuePairs.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (key == null || key.trim().isEmpty() || currentValue == null || currentValue.trim().isEmpty() || newValue == null || newValue.trim().isEmpty()) {
            throw new IllegalArgumentException("Key, current value, and new value cannot be null or empty");
        }
        if (!keyValuePairs.containsKey(key)) {
            keyValuePairs.put(key, newValue);
            return true;
        } else if (keyValuePairs.get(key).equals(currentValue)) {
            keyValuePairs.put(key, newValue);
            return true;
        }
        return false;
    }

    private String newTxn() {
        return "" + (char) ('A' + randomG.nextInt(26)) + (char) ('A' + randomG.nextInt(26));
    }

    public Set<String> getKnownNodeNames() {
        return knownNodes.keySet();
    }

    public static String byteToString(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private int calcDistance(String a, String b) {
        for (int i = 0; i < a.length(); i++) {
            int d1 = Integer.parseInt(a.substring(i, i + 1), 16);
            int d2 = Integer.parseInt(b.substring(i, i + 1), 16);
            int diff = d1 ^ d2;
            for (int bit = 3; bit >= 0; bit--) {
                if (((diff >> bit) & 1) == 1) return i * 4 + (3 - bit);
            }
        }
        return 256;
    }
}

