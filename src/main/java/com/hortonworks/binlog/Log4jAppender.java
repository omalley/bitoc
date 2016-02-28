package com.hortonworks.binlog;

import com.google.flatbuffers.FlatBufferBuilder;
import com.hortonworks.binlog.flat.Block;
import com.hortonworks.binlog.flat.Event;
import com.hortonworks.binlog.flat.ExceptionInfo;
import com.hortonworks.binlog.flat.Level;
import com.hortonworks.binlog.flat.Location;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Log4j Appender
 */
public class Log4jAppender extends AppenderSkeleton {
  // the number of event to buffer up
  private static final int QUEUE_SIZE = 32;

  // the expected size of each event
  private static final int EXPECTED_EVENT_SIZE = 256;

  public synchronized void activateOptions() {
    if (topic == null) {
      throw new IllegalArgumentException("No topic string configured");
    }
    if (brokerList == null) {
      throw new IllegalArgumentException("No broker list configured");
    }
    if (serializer != null) {
      throw new IllegalStateException("Log4jAppender already configured.");
    }
    serializer = new SerializingThread(batchSize, topic, brokerList,
        project, server, collectLocation,requiredAcks,lingerMillis,
        getErrorHandler(), queue);
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {
    queue.add(loggingEvent);
  }

  public synchronized void close() {
    if (serializer != null) {
      serializer.close();
      serializer = null;
    }
  }

  static class SerializingThread extends Thread {

    SerializingThread(int batchSize,
                      String topic,
                      String brokerList,
                      String project,
                      String server,
                      boolean collectLocation,
                      int requiredAcks,
                      int lingerMillis,
                      ErrorHandler errorHandler,
                      ArrayBlockingQueue<LoggingEvent> queue) {
      super("LoggingSerializer");
      this.batchSize = batchSize;
      this.topic = topic;
      this.brokerList = brokerList;
      this.project = project;
      this.server = server;
      this.collectLocation = collectLocation;
      this.requiredAcks = requiredAcks;
      this.lingerMillis = lingerMillis;
      this.eventArray = new int[batchSize];
      this.errorHandler = errorHandler;
      this.queue = queue;
      this.builder = new FlatBufferBuilder(EXPECTED_EVENT_SIZE * batchSize);
      startNewBlock();
    }

    void close() {
      shutdown.set(true);
    }

    private void startNewBlock() {
      eventCount = 0;
      projectOffset = project == null ? 0 : builder.createString(project);
      serverOffset = server == null ? 0 : builder.createString(server);
      hostOffset = hostName == null ? 0 : builder.createString(hostName);
      ipAddressOffset = ipAddress == null ? 0 : builder.createString(ipAddress);
    }

    static byte serializeLevel(org.apache.log4j.Level level) {
      switch (level.getSyslogEquivalent()) {
        case 0: return Level.FATAL;
        case 3: return Level.ERROR;
        case 4: return Level.WARN;
        case 6: return Level.INFO;
        case 7: return level == org.apache.log4j.Level.DEBUG ?
            Level.DEBUG : Level.TRACE;
        default:
          throw new IllegalArgumentException("Unknown level " + level);
      }
    }

    protected void flushEvents() throws IOException {
      System.out.println("Flushing " + eventCount + " events");
      Block.startEventsVector(builder, eventCount);
      for(int i=0; i < eventCount; ++i) {
        Block.addEvents(builder, eventArray[i]);
      }
      Block.finishBlockBuffer(builder,
          Block.createBlock(builder, builder.endVector()));
      ByteBuffer buffer = builder.dataBuffer();

      // for now write to a new file
      File file = new File(String.format("/tmp/%s-%05d", topic, batchNumber++));
      DataOutputStream os = new DataOutputStream(new FileOutputStream(file));
      int start = buffer.position();
      os.write(buffer.array(), start, buffer.limit() - start);
      os.close();

      builder.init(buffer);
      startNewBlock();
    }

    private void serializeEvent(LoggingEvent event) throws IOException {
      // if there is a throwable, serialize it
      String[] throwable = event.getThrowableStrRep();
      int throwableOffset = 0;
      if (throwable != null) {
        int[] strs = new int[throwable.length];
        for(int i=0; i < throwable.length; ++i) {
          strs[i] = builder.createString(throwable[i]);
        }
        int vector = ExceptionInfo.createTextVector(builder, strs);
        ExceptionInfo.startExceptionInfo(builder);
        ExceptionInfo.addText(builder, vector);
        throwableOffset = ExceptionInfo.endExceptionInfo(builder);
      }

      // if the user wants locations, serialize it
      int locationOffset = 0;
      if (collectLocation) {
        LocationInfo info = event.getLocationInformation();
        locationOffset = Location.createLocation(builder,
            builder.createString(info.getClassName()),
            builder.createString(info.getFileName()),
            builder.createString(info.getMethodName()),
            builder.createString(info.getLineNumber()));
      }
      eventArray[eventCount++] = Event.createEvent(builder,
          event.getTimeStamp(),
          serializeLevel(event.getLevel()),
          builder.createString(event.getRenderedMessage()),
          throwableOffset,
          builder.createString(event.getThreadName()),
          builder.createString(event.getLoggerName()),
          locationOffset,
          projectOffset,
          serverOffset,
          hostOffset,
          ipAddressOffset,
          pid);
    }

    public void run() {
      long nextDeadline = 0;
      while (!shutdown.get()) {
        try {
          LoggingEvent next;
          if (eventCount != 0) {
            next = queue.poll(nextDeadline - System.currentTimeMillis(),
                TimeUnit.MILLISECONDS);
          } else {
            next = queue.take();
          }
          if (next != null) {
            if (eventCount == 0) {
              nextDeadline = System.currentTimeMillis() + lingerMillis;
            }
            serializeEvent(next);
            // if the batch is full, go ahead and flush it
            if (eventCount == batchSize) {
              flushEvents();
            }
          } else {
            flushEvents();
          }
        } catch (InterruptedException e) {
          shutdown.set(true);
        } catch (Exception e) {
          errorHandler.error("Error in Appender", e, ErrorCode.WRITE_FAILURE);
        } catch (Throwable e) {
          System.err.println("Throwable in Appender: " + e.getMessage());
          e.printStackTrace();
        }
      }
    }

    // the internal state of our appender
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private int batchNumber = 0;
    private int eventCount = 0;
    private final int[] eventArray;
    private final FlatBufferBuilder builder;
    private int serverOffset;
    private int projectOffset;
    private int hostOffset;
    private int ipAddressOffset;

    // the configuration parameters
    private final String project;
    private final String server;
    private final int batchSize;
    private final boolean collectLocation;
    private final String brokerList;
    private final int requiredAcks;
    private final int lingerMillis;
    private final String topic;
    private final ErrorHandler errorHandler;
    private final ArrayBlockingQueue<LoggingEvent> queue;
  }

  public boolean requiresLayout() {
    return false;
  }

  public void setBrokerList(String value) {
    this.brokerList = value;
  }

  public void setBatchSize(int value) {
    this.batchSize = value;
  }

  public void setCollectLocation(boolean value) {
    this.collectLocation = value;
  }

  public void setRequiredAcks(int value) {
    this.requiredAcks = value;
  }

  public void setLingerMillis(int value) {
    this.lingerMillis = value;
  }

  public void setTopic(String value) {
    this.topic = value;
  }

  public void setServer(String value) {
    this.server = value;
  }

  public void setProject(String value) {
    this.project = value;
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
    System.out.println("Doing OOM static initialization");
    pid = getPid();
    InetAddress addr = null;
    try {
      addr = InetAddress.getLocalHost();
    } catch (UnknownHostException err) {
      System.err.println("Can't get localhost information - " +
          err.getMessage());
      err.printStackTrace();
    }
    ipAddress = addr == null ? "unknown" : addr.getHostAddress();
    hostName = addr == null ? "unknown" : addr.getHostName();
  }

  // the queue and thread that handles sending to Kafka
  private final ArrayBlockingQueue<LoggingEvent> queue =
      new ArrayBlockingQueue<LoggingEvent>(QUEUE_SIZE);
  private SerializingThread serializer;

  // set via configuration
  private String topic;
  private String brokerList;
  private int batchSize = 1024;
  private int requiredAcks = 1;
  private int lingerMillis = 1024;
  private String server = "client";
  private String project = "unspecified";
  private boolean collectLocation = false;
}
