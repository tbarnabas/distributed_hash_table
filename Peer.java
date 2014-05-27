
package dht;

class Peer {

public int id = 0;
public int port = 0;

public int bottomId = 0;
public int topId = 0;

public int[][] fingers = null;

public Peer(int id, int port) {
  this.id = id;
  this.port = port;
}

}
