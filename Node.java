
package dht;

import dht.Peer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public abstract class Node {

// enumeration for different operations
protected enum operations {
  CONNECT,
  INITIALIZE,
  LOOKUP,
  UPLOAD
}

// server socket
protected ServerSocket server = null;
// node instance
protected Peer instance = null;


/////////////////////////////////////////////////////////////////////////////
// communicate with node
public abstract boolean communicate(operations operation, BufferedReader input, BufferedWriter output, boolean forward) throws java.io.IOException;

/////////////////////////////////////////////////////////////////////////////
// constructor
public Node(int port) {
  // create a new node instance
  instance = new Peer(0, port);
}


/////////////////////////////////////////////////////////////////////////////
// listen on the specified port
public void listen() throws IOException {
  // create a new server socket
  server = new ServerSocket(instance.port);

  System.out.println("server is listening (port=" + instance.port + ") ...");
}


/////////////////////////////////////////////////////////////////////////////
// connect node
public boolean connect(operations operation, String host, int port, boolean forward) {
  boolean result = true;

  try {
    System.out.println("connecting to node (host=" + host + ", port=" + port + ") ...");

    // create a new client socket
    Socket node = new Socket(host, port);
  
    System.out.println("outgoing connection established");

    // create a new input reader    
    BufferedReader input = new BufferedReader(new InputStreamReader(node.getInputStream()));
    
    // create a new output writer
    BufferedWriter output = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));

    // send operation
    output.write(operation.toString());
    output.newLine();    

    System.out.println("communicate (operation=" + operation.toString() + ") ...");
    
    // communicate with client
    result = communicate(operation, input, output, forward);
    
    // flush buffer
    output.flush();
    
    // close connection
    input.close();
    output.close();

    System.out.println("outgoing connection closed");
  } catch (IOException e1) {
    result = false;
  
    System.out.println("WARNING: an exception occured during connect");
  }
  
  return (result);
}


/////////////////////////////////////////////////////////////////////////////
// accept node
public boolean accept() {
  boolean result = true;

  try {
    System.out.println("waiting for node connection ...");

    // accept a new node connection
    Socket node = server.accept();
    
    System.out.println("incoming connection established");
    
    // create a new input reader    
    BufferedReader input = new BufferedReader(new InputStreamReader(node.getInputStream()));
    
    // create a new output writer
    BufferedWriter output = new BufferedWriter(new OutputStreamWriter(node.getOutputStream()));

    // get operation
    operations operation = operations.valueOf(input.readLine());
    
    System.out.println("communicate (operation=" + operation.toString() + ") ...");

    // communicate with client
    result = communicate(operation, input, output, false);
  
    // close connection
    input.close();
    output.close();

    System.out.println("incoming connection closed");
  } catch (Exception e) {
    System.out.println("WARNING: an exception occured during accept");
  }
  
  return (result);
}


/////////////////////////////////////////////////////////////////////////////
// run node
public boolean run() {
  boolean result = true;

  while (result == true) {
    result = accept();
  }
  
  return (result);
}

} 
