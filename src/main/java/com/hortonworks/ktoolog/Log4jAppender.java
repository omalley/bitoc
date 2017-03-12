package com.hortonworks.ktoolog;

import com.google.flatbuffers.FlatBufferBuilder;
import com.hortonworks.ktoolog.flat.Block;
import com.hortonworks.ktoolog.flat.Event;
import com.hortonworks.ktoolog.flat.ExceptionInfo;
import com.hortonworks.ktoolog.flat.Level;
import com.hortonworks.ktoolog.flat.Location;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
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

  private static final String REQUIRED_ACKS_CONFIG = "request.required.acks";
  private static final String BROKER_LIST_CONFIG = "metadata.broker.list";
  private static final String CLIENT_ID_CONFIG = "client.id";
  private static final String PRODUCER_TYPE_CONFIG = "producer.type";

  public synchronized void activateOptions() {
    try {
      if (topic == null) {
        throw new IllegalArgumentException("No topic configured.");
      }
      if (!config.containsKey(BROKER_LIST_CONFIG)) {
        throw new IllegalArgumentException("No broker list configured in " +
            config.toString());
      }
      if (serializer != null) {
        throw new IllegalArgumentException("Log4jAppender already configured.");
      }
      config.put(CLIENT_ID_CONFIG, project + "-" + server + "-" + hostName);
      serializer = new SerializingThread(batchSize, topic, config,
          project, server, collectLocation, lingerMillis,
          getErrorHandler(), queue);
      serializer.start();
    } catch (Throwable e) {
      // problems in this method need to shut down the system
      // throwing causes really unintelligible error messages
      LogLog.error("Initialization error with " + config, e);
      System.exit(1);
    }
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {
    // fetch some of the properties, so they aren't lost
    loggingEvent.getThreadName();
    if (collectLocation) {
      loggingEvent.getLocationInformation();
    }
    // queue up the logging event
    try {
      queue.put(loggingEvent);
    } catch (InterruptedException e) {
      LogLog.warn("Interrupted putting into queue", e);
    }
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
                      Properties config,
                      String project,
                      String server,
                      boolean collectLocation,
                      int lingerMillis,
                      ErrorHandler errorHandler,
                      ArrayBlockingQueue<LoggingEvent> queue) {
      super("LoggingSerializer");
      this.batchSize = batchSize;
      this.topic = topic;
      this.project = project;
      this.server = server;
      this.collectLocation = collectLocation;
      this.lingerMillis = lingerMillis;
      this.eventArray = new int[batchSize];
      this.errorHandler = errorHandler;
      this.queue = queue;
      this.builder = new FlatBufferBuilder(EXPECTED_EVENT_SIZE * batchSize);
      startNewBlock();
      this.config = config;
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
      Block.startEventsVector(builder, eventCount);
      for(int i=eventCount - 1; i >= 0; --i) {
        Block.addEvents(builder, eventArray[i]);
      }
      Block.finishBlockBuffer(builder,
          Block.createBlock(builder, builder.endVector()));
      ByteBuffer buffer = builder.dataBuffer();

      // send to Kafka
      producer.send(new KeyedMessage<Void, byte[]>(topic,
          Arrays.copyOfRange(buffer.array(),
              buffer.position() + buffer.arrayOffset(),
              buffer.limit())));

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
        String lineStr = info.getLineNumber();
        int line = (lineStr == null || lineStr.equals("?"))
            ? -1 : Integer.parseInt(lineStr);
        locationOffset = Location.createLocation(builder,
            builder.createString(info.getClassName()),
            builder.createString(info.getFileName()),
            builder.createString(info.getMethodName()),
            line);
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
            if (producer == null) {
              // postpone creating the producer until log4j is configured
              // completely so we don't get warnings about non-configured
              // loggers
              producer = new Producer<Void, byte[]>(new ProducerConfig(config));
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
          LogLog.error("Throwable in Appender: " + e.getMessage());
        }
      }
      if (producer != null) {
        producer.close();
      }
    }

    // the internal state of our appender
    private AtomicBoolean shutdown = new AtomicBoolean(false);
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
    private final int lingerMillis;
    private final String topic;
    private final ErrorHandler errorHandler;
    private final ArrayBlockingQueue<LoggingEvent> queue;
    private final Properties config;
    private Producer<Void, byte[]> producer;
  }

  public boolean requiresLayout() {
    return false;
  }

  @SuppressWarnings("unused")
  public void setBrokerList(String value) {
    config.put(BROKER_LIST_CONFIG, value);
  }

  @SuppressWarnings("unused")
  public void setBatchSize(int value) {
    this.batchSize = value;
  }

  @SuppressWarnings("unused")
  public void setCollectLocation(boolean value) {
    this.collectLocation = value;
  }

  @SuppressWarnings("unused")
  public void setRequiredAcks(int value) {
    config.put(REQUIRED_ACKS_CONFIG, Integer.toString(value));
  }

  @SuppressWarnings("unused")
  public void setLingerMillis(int value) {
    this.lingerMillis = value;
  }

  @SuppressWarnings("unused")
  public void setTopic(String value) {
    this.topic = value;
  }

  @SuppressWarnings("unused")
  public void setServer(String value) {
    this.server = value;
  }

  @SuppressWarnings("unused")
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
    pid = getPid();
    InetAddress addr = null;
    try {
      addr = InetAddress.getLocalHost();
    } catch (UnknownHostException err) {
      LogLog.error("Can't get localhost information", err);
    }
    ipAddress = addr == null ? "?" : addr.getHostAddress();
    hostName = addr == null ? "?" : addr.getHostName();
  }

  // the queue and thread that handles sending to Kafka
  private final ArrayBlockingQueue<LoggingEvent> queue =
      new ArrayBlockingQueue<LoggingEvent>(QUEUE_SIZE);
  private SerializingThread serializer;

  // set via configuration
  private String topic;
  private final Properties config = new Properties();
  {
    config.put(REQUIRED_ACKS_CONFIG, "1");
    config.put(PRODUCER_TYPE_CONFIG, "async");
  }

  private int batchSize = 1024;
  private int lingerMillis = 1000;
  private String server = "client";
  private String project = "?";
  private boolean collectLocation = false;
}
