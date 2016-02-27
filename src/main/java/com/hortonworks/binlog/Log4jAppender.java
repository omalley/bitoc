package com.hortonworks.binlog;

import com.google.flatbuffers.FlatBufferBuilder;
import com.hortonworks.binlog.flat.Block;
import com.hortonworks.binlog.flat.Event;
import com.hortonworks.binlog.flat.ExceptionInfo;
import com.hortonworks.binlog.flat.Level;
import com.hortonworks.binlog.flat.Location;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * A Log4j Appender
 */
public class Log4jAppender extends AppenderSkeleton {
  static final Logger LOG = Logger.getLogger(Log4jAppender.class.getName());

  public Log4jAppender() throws IOException {
    InetAddress addr = InetAddress.getLocalHost();
    ipAddress = addr.getHostAddress();
    hostName = addr.getHostName();

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
      pid = (Integer) pid_method.invoke(mgmt);
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

  public void activateOptions() {
    if (topic == null) {
      throw new IllegalArgumentException("No topic");
    }
    eventArray = new int[blockSize];
    projectOffset = project == null ? 0 : builder.createString(project);
    serverOffset = server == null ? 0 : builder.createString(server);
    hostOffset = hostName == null ? 0 : builder.createString(hostName);
    ipAddressOffset = ipAddress == null ? 0 : builder.createString(ipAddress);
  }

  protected void flushEvents() {
    System.out.println("Flushing " + eventCount + " events");
    Block.startEventsVector(builder, eventCount);
    for(int i=0; i < eventCount; ++i) {
      Block.addEvents(builder, eventArray[i]);
    }
    Block.finishBlockBuffer(builder,
        Block.createBlock(builder, builder.endVector()));
    ByteBuffer buffer = builder.dataBuffer();
    File file = new File(String.format("/tmp/%s-%05d", topic, batchNumber++));
    try {
      DataOutputStream os = new DataOutputStream(new FileOutputStream(
          file));
      int start = buffer.position();
      os.write(buffer.array(), start, buffer.limit() - start);
      os.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Can't write file " + file, ioe);
    }

    builder.init(buffer);
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

  @Override
  protected void append(LoggingEvent loggingEvent) {
    synchronized (builder) {
      // if there is a throwable, serialize it
      String[] throwable = loggingEvent.getThrowableStrRep();
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
        LocationInfo info = loggingEvent.getLocationInformation();
        locationOffset = Location.createLocation(builder,
            builder.createString(info.getClassName()),
            builder.createString(info.getFileName()),
            builder.createString(info.getMethodName()),
            builder.createString(info.getLineNumber()));
      }
      eventArray[eventCount++] = Event.createEvent(builder,
          loggingEvent.getTimeStamp(),
          serializeLevel(loggingEvent.getLevel()),
          builder.createString(loggingEvent.getRenderedMessage()),
          throwableOffset,
          builder.createString(loggingEvent.getThreadName()),
          builder.createString(loggingEvent.getLoggerName()),
          locationOffset,
          projectOffset,
          serverOffset,
          hostOffset,
          ipAddressOffset,
          pid);

      // if the batch is full, go ahead and flush it
      if (eventCount >= blockSize) {
        flushEvents();
      }
    }
  }

  public void close() {
    if (eventCount != 0) {
      synchronized (builder) {
        flushEvents();
      }
    }
  }

  public boolean requiresLayout() {
    return false;
  }

  public void setBrokerList(String value) {
    this.brokerList = value;
  }

  public void setBlockSize(int value) {
    this.blockSize = value;
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

  // these are automatically set when we are created
  private final String hostName;
  private final String ipAddress;
  private final int pid;

  private String server;
  private String project;
  private boolean collectLocation = false;

  // the kafka parameters
  private String brokerList;
  private int requiredAcks = 1;
  private int lingerMillis = 1000;
  private String topic;

  // the internal state of our appender
  private int batchNumber = 0;
  private int eventCount = 0;
  private int blockSize = 1024;
  private int[] eventArray;
  private final FlatBufferBuilder builder = new FlatBufferBuilder(1024 * 128);
  private int serverOffset;
  private int projectOffset;
  private int hostOffset;
  private int ipAddressOffset;
}
