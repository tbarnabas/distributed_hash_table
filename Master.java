
package dht;

import dht.Node;
import dht.Peer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.Math;
import java.util.Random;

public class Master extends Node {

// array of peers
protected Peer[] peers = null;
// number of connected peers
protected int connectedPeers = 0;

// index for INITIALIZE
int index = 0;

/////////////////////////////////////////////////////////////////////////////
// create fingers
protected void createFingers(int i) {
  // create a new fingers
  peers[i].fingers = new int[16][2];
  
  System.out.println("initialize node " + (i + 1) + " (id=" + peers[i].id + ", port=" + peers[i].port + ", bottomId=" + peers[i].bottomId + ", topId=" + peers[i].topId + ") ...");
  
  // initialize fingers
  for (int j = 0; j < 16; j++) {
    peers[i].fingers[j][0] = (peers[i].id + (int)(Math.pow(2, j))) % 65536;
    peers[i].fingers[j][1] = peers[0].port;      
    
    // search next node
    boolean exit = false;
    int k = 0;
    while ((exit == false) && (k < peers.length)) {
      if (peers[k].id >= peers[i].fingers[j][0]) {
        peers[i].fingers[j][1] = peers[k].port;
        exit = true;
      } else {
        k = k + 1;
      }
    }

    System.out.println("fingers[" + (j + 1) + "]=" + peers[i].fingers[j][0] + "," + peers[i].fingers[j][1]);
  }
}


/////////////////////////////////////////////////////////////////////////////
// communicate with node
public boolean communicate(Node.operations operation, BufferedReader input, BufferedWriter output, boolean forward)  throws java.io.IOException {
  boolean result = true;
  
  if (operation == Node.operations.CONNECT) {
    if (connectedPeers < peers.length) {
      // set node port
      peers[connectedPeers].port = Integer.parseInt(input.readLine());

      System.out.println("node " + (connectedPeers + 1) + " of " + peers.length + " (port=" + Integer.toString(peers[connectedPeers].port) + ") connected");

      // increment number of connected peers
      connectedPeers = connectedPeers + 1;      

      // if all peers connected then
      if (connectedPeers == peers.length) {
        for (int i = 0; i < peers.length; i++) {
          // create fingers
          createFingers(i);
          
          // send initialization data to peer
          index = i;
          connect(Node.operations.INITIALIZE, "localhost", peers[i].port, false);
        }
      }
    } else {
      System.out.println("connection refused");
    }
  } else if (operation == Node.operations.INITIALIZE) {
    // send id
    output.write(Integer.toString(peers[index].id));
    output.newLine();
    
    // send bottom id
    output.write(Integer.toString(peers[index].bottomId));
    output.newLine();
    
    // send top id
    output.write(Integer.toString(peers[index].topId));
    output.newLine();
    
    // send fingers id and port
    for (int i = 0; i < peers[index].fingers.length; i++) {
      output.write(Integer.toString(peers[index].fingers[i][0]));
      output.newLine();
      output.write(Integer.toString(peers[index].fingers[i][1]));
      output.newLine();
    }
    
    System.out.println("initialization message sent to node " + (index + 1));
  }

  return (result);
}


/////////////////////////////////////////////////////////////////////////////
// constructor
public Master(int port, int peers) {
  super(port);
    
  // initialize array of peers
  this.peers = new Peer[peers];
  for (int i = 0; i < this.peers.length; i++) {
    Random r = new Random();

    boolean exists = false;
    int id = 0;

    // generate a unique id for every nodes
    do {    
      // generate a new id
      do {
        id = r.nextInt() % 65536;
      } while (id < 0);
      exists = false;

      // search id in finger table
      int j = 0;
      while ((exists == false) && (j < i)) {
        if (this.peers[j].id == id) {
          exists = true;
        } else {
          j = j + 1;
        }
      }
    } while (exists == true);

    // create a new peer  
    this.peers[i] = new Peer(id, 0);
  }
  
  // sort ids
  for (int i = 1; i < this.peers.length; i++) {
    int min = i - 1;
    for (int j = i; j < this.peers.length; j++) {
      if (this.peers[j].id < this.peers[min].id) {
        min = j;
      }
    }
    
    int id = this.peers[i - 1].id;
    this.peers[i - 1].id = this.peers[min].id;
    this.peers[min].id = id;
  }
  
  for (int i = 0; i < this.peers.length; i++) {
    // set bottom id
    if (i == 0) {
      this.peers[i].bottomId = this.peers[this.peers.length - 1].id + 1;
    } else {
      this.peers[i].bottomId = this.peers[i - 1].id + 1;
    }
    
    // set top id
    this.peers[i].topId = this.peers[i].id;
  }

  // set connected peers
  connectedPeers = 0;
}


/////////////////////////////////////////////////////////////////////////////
// master main
public static void main(String[] args) {
  try {
    // create a new master node with slaves number from command line
    Master master = new Master(65432, Integer.parseInt(args[0]));
  
    // listen to the specified port
    master.listen();
	
	  // run master node
    master.run();
  } catch (Exception e) {
    System.out.println("FATAL ERROR: an exception occured");
  }
}

} 
