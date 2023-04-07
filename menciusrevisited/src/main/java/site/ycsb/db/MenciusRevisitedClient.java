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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import site.ycsb.StringByteIterator;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * A database interface layer for Mencius Revisited Database.
 */
public class MenciusRevisitedClient extends DB {

  /**
  * Struct of GetReplicaListReply.
  */
  public class GetReplicaListReply {
    private List<String> replicaList;
    private boolean ready;
    private long leaderIdx;

    public GetReplicaListReply(List<String> replicaList, boolean ready, long idx) {
      this.replicaList = replicaList;
      this.ready = ready;
      this.leaderIdx = idx;
    }

    public List<String> getReplicaList() {
      return replicaList;
    }

    public boolean isReady() {
      return ready;
    }

    public long leaderIdx() {
      return leaderIdx;
    }
  }
  
  public GetReplicaListReply fromJsonString(String jsonString) {
    Pattern replicaListPattern = Pattern.compile("\"ReplicaList\":\\[(.*?)\\]");
    Matcher replicaListMatcher = replicaListPattern.matcher(jsonString);
    List<String> replicaList = new ArrayList<String>();
    while (replicaListMatcher.find()) {
      String[] parts = replicaListMatcher.group(1).split(",");
      for (String part : parts) {
        replicaList.add(part.trim().replaceAll("\"", ""));
      }
    }

    Pattern readyPattern = Pattern.compile("\"Ready\":(true|false)");
    Matcher readyMatcher = readyPattern.matcher(jsonString);
    boolean ready = false;
    if (readyMatcher.find()) {
      ready = Boolean.parseBoolean(readyMatcher.group(1));
    }

    Pattern leaderIdxPattern = Pattern.compile("\"Leader\":(\\d+)");
    Matcher leaderIdxMatcher = leaderIdxPattern.matcher(jsonString);
    long idx = 0;
    if (leaderIdxMatcher.find()) {
      idx = Long.parseLong(leaderIdxMatcher.group(1));
    }

    return new GetReplicaListReply(replicaList, ready, idx);
  }

  private Socket cli;

  public static Socket newClient(String addr) throws Exception {
    String[] parts = addr.split(":");
    String ip = parts[0];
    int port = Integer.parseInt(parts[1]);
    InetAddress address = InetAddress.getByName(ip);
    // Socket socket = new Socket(address, port);

    // This is for local Docker testing
    Socket socket = new Socket("", port);
    socket.setTcpNoDelay(true);
    return socket;
  }

  private GetReplicaListReply resp;

  private static class Propose implements Serializable {
    private int commandId;
    private Command command;
    private long timestamp;

    public Propose(int commandId, Command command, long timestamp) {
      this.commandId = commandId;
      this.command = command;
      this.timestamp = timestamp;
    }

    public void marshal(OutputStream out) throws IOException {
      byte[] bs = new byte[8];
      byte[] b = new byte[4];
      int tmp32 = commandId;
      byte[] start = new byte[1];
      start[0] = 0;
      out.write(start, 0, 1);
      b[0] = (byte) (tmp32);
      b[1] = (byte) (tmp32 >> 8);
      b[2] = (byte) (tmp32 >> 16);
      b[3] = (byte) (tmp32 >> 24);
      out.write(b, 0, 4);

      // Write command
      command.marshal(out);

      long tmp64 = timestamp;
      bs[0] = (byte) (tmp64);
      bs[1] = (byte) (tmp64 >> 8);
      bs[2] = (byte) (tmp64 >> 16);
      bs[3] = (byte) (tmp64 >> 24);
      bs[4] = (byte) (tmp64 >> 32);
      bs[5] = (byte) (tmp64 >> 40);
      bs[6] = (byte) (tmp64 >> 48);
      bs[7] = (byte) (tmp64 >> 56);
      out.write(bs);
    }
  }

  private static class ProposeReplyTS implements Serializable {
    private byte ok;
    private int commandId;
    private String value;
    private long timestamp;

    public void unmarshal(DataInputStream wire) throws IOException {
      byte[] bs = new byte[8];

      wire.readFully(bs, 0, 5);
      ok = bs[0];
      commandId = ((bs[1] & 0xFF) | ((bs[2] & 0xFF) << 8) | ((bs[3] & 0xFF) << 16) | ((bs[4] & 0xFF) << 24));

      wire.readFully(bs, 0, 8);

      long vl = ByteBuffer.wrap(bs).getLong();
      byte[] data = new byte[(int) vl];

      wire.readFully(data, 0, (int) vl);
      value = new String(data);
      wire.readFully(bs, 0, 8);

      timestamp = ((long) (bs[0] & 0xFF) | ((long) (bs[1] & 0xFF) << 8) | ((long) (bs[2] & 0xFF) << 16)
          | ((long) (bs[3] & 0xFF) << 24) | ((long) (bs[4] & 0xFF) << 32) | ((long) (bs[5] & 0xFF) << 40)
          | ((long) (bs[6] & 0xFF) << 48) | ((long) (bs[7] & 0xFF) << 56));
    }
  }

  private static class Command implements Serializable {
    private byte op;
    private long k;
    private String v;

    public Command(byte op, long k, String v) {
      this.op = op;
      this.k = k;
      this.v = v;
    }

    public void marshal(OutputStream out) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(8);

      // Write op
      out.write(op);

      // Write k
      buffer.putLong(0, k);
      out.write(buffer.array());

      // Write v length
      long vl = v.length();
      buffer.putLong(0, vl);
      out.write(buffer.array());

      // Write v
      out.write(v.getBytes());
    }
  }

  public ProposeReplyTS sendRequestAndAwaitForReply(byte typeID, int cmdID, long key, String value) {
    try {
      OutputStream outToServer = cli.getOutputStream();
      InputStream inFromServer = cli.getInputStream();

      // Create and send Propose
      Command cmd = new Command(typeID, key, value);
      Propose propose = new Propose(cmdID, cmd, System.currentTimeMillis());
      DataOutputStream dos = new DataOutputStream(outToServer);
      propose.marshal(dos);
      dos.flush();

      // Receive ProposeReplyTS
      DataInputStream dis = new DataInputStream(inFromServer);
      ProposeReplyTS reply = new ProposeReplyTS();
      reply.unmarshal(dis);
      return reply;
    } catch (IOException e) {
      // e.printStackTrace();
      try{
        cli.close();
      }catch(Exception e1){
        // donothing
      }     
      reinit();
      return sendRequestAndAwaitForReply(typeID, cmdID, key, value);
    }
  }

  public void reinit() {
    try {
      Random random = new Random();
      int newLeaderID = random.nextInt(addrs.length);
      cli = newClient(addrs[newLeaderID]);
    } catch (Exception e) {
      // e.printStackTrace();
    }
  }
  private String[] addrs;
  @Override
  public void init() throws DBException {
    try {
      
      Properties props = getProperties();
      String myCustomParamString = props.getProperty("valuelength");
      if (myCustomParamString != null) {
        try {
          valueLength = Integer.parseInt(myCustomParamString);
        } catch (NumberFormatException e) {
          throw new DBException("Invalid value for my_custom_param: " + myCustomParamString, e);
        }
      }

      URL url = new URL("http://localhost:8080/replicaList");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);

      String requestParam1 = "{\"methodName\":\"Master.GetReplicaList\",\"body\":\"{\\\"Replic";
      String requestParam2="aList\\\":[],\\\"Ready\\\":false,\\\"Leader\\\":0}\"}";
      String requestParam = requestParam1+requestParam2;
      Writer writer = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
      writer.write(requestParam);
      writer.close();

      BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
      StringBuilder response = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
      reader.close();

      String jsonResponse = response.toString();
      resp = fromJsonString(jsonResponse);
      addrs = resp.replicaList.toArray(new String[resp.replicaList.size()]);
      cli = newClient(addrs[(int) resp.leaderIdx]);
    } catch (Exception e) {
      System.out.println(e);
    } finally {
      // do nothing, need to write something to compile.
    }
  }

  private int commandID = 0;
  private int valueLength = 256;

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

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    long k= Long.parseLong(key.replace("user", ""));
    ProposeReplyTS reply = sendRequestAndAwaitForReply((byte)2, getcommandID(), k, "");
    if (reply == null) {
      return Status.SERVICE_UNAVAILABLE;
    }
    if (reply.ok!=1){
      return Status.ERROR;
    }
    if (reply.value == "") {
      result.put(Long.toString(k), new StringByteIterator(reply.value));
      return Status.NOT_FOUND;
    }
    result.put(Long.toString(k), new StringByteIterator(reply.value));
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
    try {
      String value = randomString(valueLength);
      long k= Long.parseLong(key.replace("user", ""));
      ProposeReplyTS reply = sendRequestAndAwaitForReply((byte)1, getcommandID(), k, value);
      if (reply==null){
        return Status.ERROR;
      }
      if (reply.ok!=1){
        return Status.ERROR;
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
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
