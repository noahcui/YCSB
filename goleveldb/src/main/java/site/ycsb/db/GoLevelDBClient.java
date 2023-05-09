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
* A database interface layer for goleveldb Revisited Database.
*/
public class GoLevelDBClient extends DB {

  /**
 * Struct of GetReplicaListReply.
 */
  public class Command {
    public static final byte PUT = 0;
    public static final byte GET = 1;
    public static final byte ERR = 3;

    private byte commandType;
    private long commandID;
    private long clientID;
    private String key;
    private String value;
    
    public Command() {
      this.commandType = GET;
      this.commandID = 0;
      this.clientID = 0;
      this.key = "";
      this.value = "";
    }
    
    public Command(byte commandType, long commandID, long clientID, String key, String value) {
      this.commandType = commandType;
      this.commandID = commandID;
      this.clientID = clientID;
      this.key = key;
      this.value = value;
    }

    public void marshal(OutputStream wire) throws IOException {
      long kl = key.length();
      long vl = value.length();
  
      wire.write(commandType);
      writeLong(wire, commandID);
      writeLong(wire, clientID);
      writeLong(wire, kl);
      writeLong(wire, vl);
  
      wire.write(key.getBytes(StandardCharsets.UTF_8));
      wire.write(value.getBytes(StandardCharsets.UTF_8));
    }
    
    public void unmarshal(InputStream wire) throws IOException {
      commandType = (byte) wire.read();
  
      commandID = readLong(wire);
      clientID = readLong(wire);
  
      long kl = readLong(wire);
      long vl = readLong(wire);
  
      byte[] keyBytes = new byte[(int) kl];
      wire.read(keyBytes);
      key = new String(keyBytes, StandardCharsets.UTF_8);
  
      byte[] valueBytes = new byte[(int) vl];
      wire.read(valueBytes);
      value = new String(valueBytes, StandardCharsets.UTF_8);
    }
  
    private void writeLong(OutputStream out, long v) throws IOException {
      for (int i = 0; i < 8; i++) {
        out.write((int) (v & 0xff));
        v >>= 8;
      }
    }
    
    private long readLong(InputStream in) throws IOException {
      long v = 0;
      for (int i = 0; i < 8; i++) {
        v |= (((long) in.read()) & 0xff) << (8 * i);
      }
      return v;
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
    socket.setSoTimeout(2000);
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
      String addr = "localhost:7070";
      cli = newClient(addr);
      Random random = new Random();
      cliID = random.nextInt();
      // warming up
      for (int i=0; i<10; i++){
        sendRequestAndAwaitForReply(Command.GET, getcommandID(), "warmup", "");
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
    try{
      cli.close();
    }catch(Exception e){
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

  public Command sendRequestAndAwaitForReply(byte typeID, int cmdID, String key, String value) {
    try {
      OutputStream outToServer = cli.getOutputStream();
      InputStream inFromServer = cli.getInputStream();

      // Create and send Propose
      Command cmd = new Command(typeID, cmdID, cliID, key, value);

      DataOutputStream dos = new DataOutputStream(outToServer);
      cmd.marshal(dos);
      dos.flush();

      // Receive ProposeReplyTS
      DataInputStream dis = new DataInputStream(inFromServer);
      Command reply = new Command();
      reply.unmarshal(dis);
      return reply;
    } catch (IOException e) {
      // e.printStackTrace();
      // System.out.println(cliID);
      try{
        cli.close();
      }catch(Exception e1){
        // donothing
      }     
      return null;
      // return sendRequestAndAwaitForReply(typeID, cmdID, key, value);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    
    Command reply=sendRequestAndAwaitForReply(Command.GET, getcommandID(), key, "");
    if (reply != null && reply.commandType == Command.GET){
      return Status.OK;
    }
    return Status.ERROR;
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
    Command reply=sendRequestAndAwaitForReply(Command.PUT, getcommandID(), key, value);
    if (reply != null && reply.commandType == Command.PUT){
      return Status.OK;
    }
    return Status.ERROR;
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
