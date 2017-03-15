/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bitoc;

import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.bitoc.flat.Block;
import org.apache.bitoc.flat.Event;
import org.apache.bitoc.flat.ExceptionInfo;
import org.apache.bitoc.flat.Location;
import org.apache.bitoc.flat.StringPair;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Serialize the events.
 */
public class EventSerializer implements AutoCloseable {

  private final Producer producer;
  private final int lingerMillis;
  private final String server;
  private final String project;
  private final int[] eventArray;
  private final FlatBufferBuilder builder;

  private int eventCount = 0;
  private int projectOffset;
  private int serverOffset;
  private int hostOffset;
  private int ipAddressOffset;


  // the expected size of each event
  private static final int EXPECTED_EVENT_SIZE = 256;

  public EventSerializer(Producer producer,
                         int batchSize,
                         int lingerMillis,
                         String server,
                         String project) {
    this.producer = producer;
    this.eventArray = new int[batchSize];
    this.lingerMillis = lingerMillis;
    this.server = server;
    this.project = project;
    this.builder = new FlatBufferBuilder(EXPECTED_EVENT_SIZE * batchSize);
    startNewBlock();
  }

  /**
   * Append a logging event
   * @param time the time that the event occurred
   * @param level the level of the event (error, warn, info, debug, etc.)
   * @param logger the name of the logger
   * @param message the message to log
   * @param location an optional location to include
   * @param error an optional related error to the event
   * @param mdc the optional diagnostic context
   * @throws IOException
   */
  public synchronized void append(long time,
                                  byte level,
                                  String logger,
                                  String message,
                                  StackTraceElement[] location,
                                  Throwable error,
                                  Map<String, String> mdc) throws IOException {
    int throwableOffset = 0;
    if (error != null) {
      throwableOffset = serializeException(error);
    }

    int locationOffset = 0;
    if (location != null) {
      locationOffset = serializeLocation(location);
    }
    String threadName = Thread.currentThread().getName();

    int mdcOffset = 0;
    if (mdc != null) {
      mdcOffset = serializeMdc(mdc);
    }
    eventArray[eventCount++] = Event.createEvent(builder,
        time,
        level,
        builder.createString(message),
        throwableOffset,
        builder.createString(threadName),
        builder.createString(logger),
        locationOffset,
        projectOffset,
        serverOffset,
        hostOffset,
        ipAddressOffset,
        pid,
        mdcOffset);

    // If we are full, go ahead and flush
    if (eventCount == eventArray.length) {
      flushEvents();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (eventCount != 0) {
      flushEvents();
    }
    producer.close();
  }

  void startNewBlock() {
    eventCount = 0;
    projectOffset = project == null ? 0 : builder.createString(project);
    serverOffset = server == null ? 0 : builder.createString(server);
    hostOffset = hostName == null ? 0 : builder.createString(hostName);
    ipAddressOffset = ipAddress == null ? 0 : builder.createString(ipAddress);
  }

  void flushEvents() throws IOException {
    Block.startEventsVector(builder, eventCount);
    for (int i = eventCount - 1; i >= 0; --i) {
      Block.addEvents(builder, eventArray[i]);
    }
    Block.finishBlockBuffer(builder,
        Block.createBlock(builder, builder.endVector()));
    ByteBuffer buffer = builder.dataBuffer();

    // send to Kafka
    producer.send(builder.sizedByteArray());

    builder.init(buffer);
    startNewBlock();
  }

  int serializeLocation(StackTraceElement[] locations) {
    int[] offsets = new int[locations.length];
    for (int i = 0; i < locations.length; ++i) {
      offsets[i] = Location.createLocation(builder,
          builder.createString(locations[i].getClassName()),
          builder.createString(locations[i].getFileName()),
          builder.createString(locations[i].getMethodName()),
          locations[i].getLineNumber());
    }
    return Event.createLocationVector(builder, offsets);
  }

  int serializeException(Throwable throwable) throws IOException {
    Throwable cause = throwable.getCause();
    int causeOffset = 0;
    if (cause != null) {
      causeOffset = serializeException(cause);
    }
    int locationOffset = serializeLocation(throwable.getStackTrace());
    return ExceptionInfo.createExceptionInfo(builder,
        builder.createString(throwable.getMessage()),
        locationOffset, causeOffset);
  }

  int serializeMdc(Map<String, String> mdc) {
    int[] pairs = new int[mdc.size()];
    int idx = 0;
    for(Map.Entry<String, String> entry: mdc.entrySet()) {
      pairs[idx++] = StringPair.createStringPair(builder,
          builder.createString(entry.getKey()),
          builder.createString(entry.getValue()));
    }
    return Event.createMdcVector(builder, pairs);
  }

  class FlushThread extends Thread {
    private boolean isSet = false;
    private long nextFlush = 0;

    FlushThread() {
      super("Bitoc flush thread");
      setDaemon(true);
    }

    synchronized void cancelTimeout() {
      isSet = false;
      this.notify();
    }

    synchronized void setTimeout(long millis) {
      isSet = true;
      nextFlush = System.currentTimeMillis() + millis;
      this.notify();
    }

    public void run() {
      while (true) {
        try {
          synchronized (this) {
            if (isSet) {
              long current = System.currentTimeMillis();
              while (isSet && nextFlush > current) {
                this.wait(nextFlush - current);
                current = System.currentTimeMillis();
              }
              if (isSet) {
                EventSerializer.this.flushEvents();
              }
              isSet = false;
            } else {
              this.wait();
            }
          }
        } catch (InterruptedException ie) {
          break;
        } catch (Throwable th) {
          System.err.println("Caught exception in flush thread");
          th.printStackTrace(System.err);
        }
      }
    }
  }

  static int getPid() {
    // get the pid... stupid java.. hack
    try {
      java.lang.management.RuntimeMXBean runtime =
          java.lang.management.ManagementFactory.getRuntimeMXBean();
      java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
      jvm.setAccessible(true);
      sun.management.VMManagement mgmt =
          (sun.management.VMManagement) jvm.get(runtime);
      java.lang.reflect.Method pid_method =
          mgmt.getClass().getDeclaredMethod("getProcessId");
      pid_method.setAccessible(true);
      return (Integer) pid_method.invoke(mgmt);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Can't get PID", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Can't get PID", e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Can't get PID", e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Can't get PID", e);
    }
  }


  // get the static information about this host
  static final int pid;
  static final String hostName;
  static final String ipAddress;
  static {
    pid = getPid();
    InetAddress addr = null;
    try {
      addr = InetAddress.getLocalHost();
    } catch (UnknownHostException err) {
      System.err.println("ERROR: Can't get localhost information.");
      err.printStackTrace(System.err);
    }
    ipAddress = addr == null ? null : addr.getHostAddress();
    hostName = addr == null ? null : addr.getHostName();
  }
}
