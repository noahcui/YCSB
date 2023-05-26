/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.*;
import java.net.*;
import java.util.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A database interface layer for my Database.
 */
public class MyClient extends DB {

  // Replica list
  private String[] replicaList={"localhost:10001", "localhost:11001", "localhost:12001"};
  /**
   * Struct of GetReplicaListReply.
   */
  public class Command {
    public static final int GET = 0;
    public static final int PUT = 1;
    public static final int DEL = 2;
    public static final int OK = 0;
    public static final int REJECT = 1;
    public static final String RETRY = "retry";
    public static final String BAD="bad command";
    public static final String LEADERIS="leader is ...";
    public static final String KEYNOTFOUND="key not found";
    public static final String EMPTY="";
    private int type;
    private String key;
    private String value;

    public Command() {
      this.type = GET;
      this.key = "";
      this.value = "";
    }
  
    public Command(int type, String key, String value) {
      this.type = type;
      this.key = key;
      this.value = value;
    }
  
    public void marshal(OutputStream wire) throws IOException {
      String cmdStr = this.getCommandString() + "\n";
      wire.write(cmdStr.getBytes(StandardCharsets.UTF_8));
      wire.flush();
    }
  
    public String unmarshal(InputStream wire) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(wire, StandardCharsets.UTF_8));
      String cmdStr = reader.readLine();

      return cmdStr;
    }
  
    private String getCommandString() {
      String cmdTypeStr;
      if (type == GET) {
        cmdTypeStr = "get";
      } else if (type == PUT) {
        cmdTypeStr = "put";
      } else {
        cmdTypeStr = "del";
      }
      return cmdTypeStr + " " + key + " " + value;
    }
  }

  private Socket cli;

  public static Socket newClient(String addr) throws Exception {
    String[] parts = addr.split(":");
    String ip = parts[0];
    int port = Integer.parseInt(parts[1]);
    InetAddress address = InetAddress.getByName(ip);
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress(address, port), 1000);
    // socket.setSoTimeout(999999999);
    socket.setTcpNoDelay(true);
    return socket;
  }

  private int cliID;

  @Override
  public void init() throws DBException {
    try {
      Properties props = getProperties();
      String myCustomParamString = props.getProperty("valuesize");
      if (myCustomParamString != null) {
        try {
          valueSize = Integer.parseInt(myCustomParamString);
        } catch (NumberFormatException e) {
          throw new DBException("Invalid value for my_custom_param: " + myCustomParamString, e);
        }
      }
      
      Random random = new Random();
      cliID = random.nextInt();
      int sid = Math.abs(cliID) % 3;
      String addr = replicaList[1];
      cli = newClient(addr);
      String reply1 = sendRequestAndAwaitForReply(Command.PUT, getcommandID(), "warmup", "hello");
      System.out.println(reply1);
      // warming up
      for (int i = 0; i < 10; i++) {
        
        String reply = sendRequestAndAwaitForReply(Command.GET, getcommandID(), "warmup", "hello");
        System.out.println(reply);
      }
    } catch (Exception e) {
      System.out.println(e);
    } finally {
      // do nothing, need to write something to compile.
    }
  }

  private int commandID = 0;
  private int valueSize = 256;

  @Override
  public void cleanup() throws DBException {
    try {
      cli.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public int getcommandID() {
    int toret = commandID;
    commandID++;
    return toret;
  }

  public static String randomString(int length) {
    Random random = new Random();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      builder.append((char) (random.nextInt(26) + 'a'));
    }
    return builder.toString();
  }

  public String sendRequestAndAwaitForReply(int typeID, int cmdID, String key, String value) {
    try {
      OutputStream outToServer = cli.getOutputStream();
      InputStream inFromServer = cli.getInputStream();

      // Create and send Propose
      Command cmd = new Command(typeID, key, value);
      
      DataOutputStream dos = new DataOutputStream(outToServer);
      cmd.marshal(dos);
      dos.flush();

      // Receive ProposeReplyTS
      DataInputStream dis = new DataInputStream(inFromServer);
      Command reply = new Command();
      String msg=reply.unmarshal(dis);
      // if(msg == null){
      //   return Command.EMPTY;
      // }
      if(msg.equals(Command.RETRY)){
        return sendRequestAndAwaitForReply(typeID, cmdID, key, value);
      }
      return msg;
    } catch (IOException e) {
      e.printStackTrace();
      // System.out.println(cliID);
      try {
        cli.close();
      } catch (Exception e1) {
        // donothing
      }
      return null;
      // return sendRequestAndAwaitForReply(typeID, cmdID, key, value);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    String reply = sendRequestAndAwaitForReply(Command.GET, getcommandID(), key, "");
    if (reply ==Command.LEADERIS) {
      return Status.ERROR;
    }
    if (reply == Command.BAD) {
      return Status.BAD_REQUEST;
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // need an return value, do nothing
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String value = randomString(valueSize);
    String reply = sendRequestAndAwaitForReply(Command.PUT, getcommandID(), key, value);
    if (reply ==Command.LEADERIS) {
      return Status.ERROR;
    }
    if (reply == Command.BAD) {
      return Status.BAD_REQUEST;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    // need an return value, do nothing
    return Status.NOT_IMPLEMENTED;
  }

}
