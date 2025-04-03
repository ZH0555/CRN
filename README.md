Build Instructions
==================

1. Clone the repository
- git clone https://github.com/ZH0555/CRN.git
- cd CRN1

2. Build the project
- mvn clean install


Working Functionality
=====================

The CRN (Censorship Resistant Network) is a distributed P2P network where each node acts as both a key/value store and a relay point for messages. These nodes communicate via UDP packets and the network ensures censorship resistance through communication requests through various nodes

- There is no central server, so information is stored across multiple nodes allowing for distributed access to data
- Nodes relay messages to other nodes which ensures anonymity
- Data is stored on the nodes closest to the key's hashID, allowing for travel efficiency

Node communication includes:
- Name requests, where a node can request the name of another node to check if it's acceptable
- Key/Value operations, nodes can write, request or perform compare and swap (CAS) operations on stored key/value pairs
- UDP based communication, where this protocol is designed to handle out of order message delivery through transaction IDs.

