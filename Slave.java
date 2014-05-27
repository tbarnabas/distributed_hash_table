
package bead.dht;

import bead.dht.Crc16;
import bead.dht.Node;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.Integer;
import java.lang.Thread;
import java.util.Hashtable;

public class Slave extends Node {

// has table for contents
protected Hashtable<String, String> contents = null;

// forwarded input
String forwardedKey = null;
BufferedReader forwardedInput = null;
BufferedWriter forwardedOutput = null;


/////////////////////////////////////////////////////////////////////////////
// lookup port
int lookupPort(int crc) {
  int result = 0;
  boolean find = false;
  
  if (instance.bottomId < instance.topId) {
    if ((instance.bottomId <= crc) && (crc <= instance.topId)) {
	  result = instance.port;
	  find = true;
	}
  } else {
    if ((instance.bottomId <= crc) || (crc <= instance.topId)) {
	  result = instance.port;
	  find = true;
	}
  }
  
  if (find == false) {
    if ((instance.id < crc) && (crc <= instance.fingers[0][0])) {
	  result = instance.fingers[0][1];
	} else {
      int i = 0;
	  while ((find == false) && (i < (instance.fingers.length - 1))) {
	    if (instance.fingers[i][0] < instance.fingers[i + 1][0]) {
		  if ((instance.fingers[i][0] <= crc) && (crc < instance.fingers[i + 1][0])) {
		    result = instance.fingers[i][1];
		    find = true;
		  }
	    } else {
		  if ((instance.fingers[i][0] <= crc) || (crc < instance.fingers[i + 1][0])) {
		    result = instance.fingers[i][1];
		    find = true;
		  }
	    }

	    i = i + 1;
	  }

	  if (find == false) {
	    result = instance.fingers[instance.fingers.length - 1][1];
	  }
    }
  }
  
  return (result);
}

/////////////////////////////////////////////////////////////////////////////
// communicate with node
public boolean communicate(Node.operations operation, BufferedReader input, BufferedWriter output, boolean forward) throws java.io.IOException {
  boolean result = true;
  
  if (operation == Node.operations.CONNECT) {
    // send port
    output.write(Integer.toString(instance.port));
    output.newLine();
    
    System.out.println("node address (port=" + Integer.toString(instance.port) + ") sent");
  } else if (operation == Node.operations.INITIALIZE) {
    // receive id
    instance.id = Integer.parseInt(input.readLine());

    // receive bottom id
    instance.bottomId = Integer.parseInt(input.readLine());
    
    // receive top id
    instance.topId = Integer.parseInt(input.readLine());

    System.out.println("node (id=" + instance.id + ", port=" + instance.port + ", bottomId=" + instance.bottomId + ", topId=" + instance.topId + ") ...");  

    // receive fingers
    instance.fingers = new int[16][2];
    for (int i = 0; i < instance.fingers.length; i++) {
      instance.fingers[i][0] = Integer.parseInt(input.readLine());
      instance.fingers[i][1] = Integer.parseInt(input.readLine());
      
      System.out.println("fingers[" + (i + 1) + "]=" + instance.fingers[i][0] + "," + instance.fingers[i][1]);
    }
      
    System.out.println("initialized");
  } else if (operation == Node.operations.UPLOAD) {
    if (forward == false) {
      // receive key and calculate crc
      String key = input.readLine();
	  int crc = Crc16.crc(key);
	  
      System.out.println("uploading content (key=" + key + ", crc=" + crc + ") ...");
      
      // lookup port for crc
      int port = lookupPort(crc);
      
      if (port == instance.port) {
        String content = "";
        
        // read all content
        int data = 0;
        do {
          data = input.read();
          if (data != -1) {
            content = content + (char)data;
          }
        } while (data != -1);
        
        // store content in hash table
        contents.put(key, content);
              
        System.out.println(content.length() + " byte(s) stored");
      } else {
        forwardedKey = key;
        forwardedInput = input;
        connect(Node.operations.UPLOAD, "localhost", port, true);
      }
    } else {
      // send forwarded key
      output.write(forwardedKey);
      output.newLine();
	  
	  // flush output
	  output.flush();
      
      // forward all content
      int data = 0;
      do {
        data = forwardedInput.read();
        if (data != -1) {
          output.write((char)data);
        }
      } while (data != -1);

      System.out.println("content forwarded");
    }
  } else if (operation == Node.operations.LOOKUP) {
    if (forward == false) {
      // receive key and calculate crc
      String key = input.readLine();
	  int crc = Crc16.crc(key);
      
      System.out.println("looking up content (key=" + key + ", crc=" + crc + ") ...");
      
      // lookup port for crc
      int port = lookupPort(crc);
      
      if (port == instance.port) {
	    if (contents.containsKey(key) == true) {
		  output.write("found");
		  output.newLine();

          // flush output
	      output.flush();
		  
		  // write content
		  output.write(contents.get(key));
		  
          // flush output
	      output.flush();

 		  System.out.println("content found");
		} else {
		  output.write("not-found");
		  output.newLine();

          // flush output
	      output.flush();

		  System.out.println("content not found");
		}	
      } else {
        forwardedKey = key;
        forwardedOutput = output;
        connect(Node.operations.LOOKUP, "localhost", port, true);
      }
    } else {
      // send forwarded key
      output.write(forwardedKey);
      output.newLine();
	  
	  // flush output
	  output.flush();
      
      // forward all content
      int data = 0;
      do {
        data = input.read();
        if (data != -1) {
          forwardedOutput.write((char)data);
        }
      } while (data != -1);
	  
	  forwardedOutput.flush();

      System.out.println("content forwarded");
    }
  }

  return (result);
}


/////////////////////////////////////////////////////////////////////////////
// constructor
public Slave(int port) {
  super(port);

  // create a new hash table for contents
  contents = new Hashtable<String, String>();
}


/////////////////////////////////////////////////////////////////////////////
// slave main
public static void main(String[] args) {
  try {
    // create a new slave node with port number from command line
    Slave slave = new Slave(Integer.parseInt(args[0]));
	
    // listen to the specified port
    slave.listen();
    
    // connecting to master node
    slave.connect(Node.operations.CONNECT, "localhost", 65432, false);
	
	  // run slave node
    slave.run();
  } catch (Exception e) {
    System.out.println("FATAL ERROR: an exception occured");
  }
}

} 
