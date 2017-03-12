package org.apache.bitoc;

import org.apache.bitoc.flat.Block;
import org.apache.bitoc.flat.Event;
import org.apache.bitoc.flat.ExceptionInfo;
import org.apache.bitoc.flat.Level;
import org.apache.bitoc.flat.Location;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Print the serialized event blobs.
 */
public class EventPrinter {

  static final Logger LOG = Logger.getLogger(EventPrinter.class);

  static final SimpleDateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");

  public static void printBlock(PrintStream out,
                                Block eventBlock) throws IOException {
    Event staticEvent = new Event();
    Location staticLocation = new Location();
    ExceptionInfo staticException = new ExceptionInfo();
    int eventCount = eventBlock.eventsLength();
    for(int i=0; i < eventCount; ++i) {
      Event event = eventBlock.events(staticEvent, i);
      out.print("{\"time\": \"");
      out.print(DATE_FORMAT.format(new Date(event.time())));
      out.print("\", \"level\": \"");
      out.print(Level.name(event.level()));
      out.print("\", \"message\": \"");
      out.print(event.message());
      out.print("\", \"thread\": \"");
      out.print(event.thread());
      out.print("\", \"logger\": \"");
      out.print(event.logger());
      out.print("\", ");
      Location location = event.location(staticLocation);
      if (location != null) {
        out.print("\"location\": {\"class\": \"");
        out.print(location.className());
        out.print("\", \"file\": \"");
        out.print(location.fileName());
        out.print("\", \"method\": \"");
        out.print(location.methodName());
        out.print("\", \"line\": ");
        out.print(location.line());
        out.print("}, ");
      }
      ExceptionInfo exceptionInfo = event.throwable(staticException);
      if (exceptionInfo != null) {
        out.print("\"throwable\": [");
        int len = exceptionInfo.textLength();
        for(int j=0; j < len; ++j) {
          if (j != 0) {
            out.print(", ");
          }
          out.print("\"");
          out.print(exceptionInfo.text(j));
          out.print("\"");
        }
        out.print("], ");
      }
      out.print("\"project\": \"");
      out.print(event.project());
      out.print("\", \"server\": \"");
      out.print(event.server());
      out.print("\", \"host\": \"");
      out.print(event.hostName());
      out.print("\", \"ip\": \"");
      out.print(event.ipAddress());
      out.print("\", \"pid\": ");
      out.print(event.pid());
      out.println("}");
    }
  }

  static final int TIMEOUT = 60 * 1000;
  static final int METADATA_BUFFER = 64 * 1024;
  static final int DATA_SIZE = 128 * 1024;
  static final int ERROR_LIMIT = 5;

  static List<PartitionMetadata> getMetadata(String brokers,
                                             String topic,
                                             String clientId
                                             ) throws IOException {
    for(String serverName: brokers.split(",")) {
      try {
        String[] parts = serverName.split(":");
        SimpleConsumer consumer = new SimpleConsumer(parts[0],
            Integer.parseInt(parts[1]), TIMEOUT, METADATA_BUFFER, clientId);
        TopicMetadataResponse response =
            consumer.send(new TopicMetadataRequest
                (Collections.singletonList(topic)));
        consumer.close();
        return response.topicsMetadata().get(0).partitionsMetadata();
      } catch (Exception e) {
        LOG.warn("Can't access " + serverName, e);
      }
    }
    throw new IOException("Can't get metadata from " + brokers);
  }

  static class HostPort {
    final String host;
    final int port;
    HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public boolean equals(Object other) {
      return this == other ||
          (this.getClass() == other.getClass() &&
              toString().equals(other.toString()));
    }

    @Override
    public int hashCode() {
      return host.hashCode() + port;
    }

    @Override
    public String toString() {
      return host + ":" + port;
    }
  }

  static class RangeReader {
    RangeReader(PartitionMetadata partitionMetadata,
                String topic,
                long fromDate,
                long toDate,
                String clientName){
      this.topic = topic;
      this.clientName = clientName;
      this.partition = partitionMetadata.partitionId();
      this.fromDate = fromDate;
      this.toDate = toDate;
      updateKnownHosts(partitionMetadata);
      this.consumer = new SimpleConsumer(partitionMetadata.leader().host(),
          partitionMetadata.leader().port(), TIMEOUT, METADATA_BUFFER,
          clientName);
      currentOffset = getStartOffset();
    }

    void updateKnownHosts(PartitionMetadata metadata) {
      for(int i=0; i < metadata.replicas().size(); ++i) {
        knownHosts.add(new HostPort(metadata.replicas().get(i).host(),
            metadata.replicas().get(i).port()));
      }
      leader = new HostPort(metadata.leader().host(), metadata.leader().port());
    }

    long getStartOffset() {
      Map<TopicAndPartition, PartitionOffsetRequestInfo> info =
          new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      info.put(new TopicAndPartition(topic, partition),
          new PartitionOffsetRequestInfo(fromDate, 1));
      OffsetResponse response =
          consumer.getOffsetsBefore(new OffsetRequest(info,
              kafka.api.OffsetRequest.CurrentVersion(), clientName));
      if (response.hasError()) {
        LOG.warn("Error fetching data offset. Reason: " +
            response.errorCode(topic, partition));
        return 0;
      }
      long result = response.offsets(topic, partition)[0];
      LOG.info("Using offset " + result + " for " + topic +
          " part " + partition);
      return result;
    }

    void findNewLeader() throws IOException, InterruptedException {
      for(int retry=0; retry < ERROR_LIMIT; retry++) {
        SimpleConsumer metaConsumer = null;
        for(HostPort hp: knownHosts) {
          try {
            metaConsumer = new SimpleConsumer(hp.host, hp.port,
                TIMEOUT, METADATA_BUFFER, clientName);
            TopicMetadataResponse response = metaConsumer.send(
                new TopicMetadataRequest(Collections.singletonList(topic)));
            metaConsumer.close();
            metaConsumer = null;
            for(TopicMetadata metadata: response.topicsMetadata()) {
              for(PartitionMetadata part: metadata.partitionsMetadata()) {
                if (part.partitionId() == partition && part.leader() != null) {
                  HostPort newLeader = new HostPort(part.leader().host(),
                      part.leader().port());
                  if (!newLeader.equals(leader) || retry != 0) {
                    updateKnownHosts(part);
                    return;
                  }
                }
              }
            }
          } catch (Exception e) {
            LOG.warn("Problem finding new leader", e);
            if (metaConsumer != null) {
              metaConsumer.close();
            }
          }
        }
        Thread.sleep(1000);
      }
      throw new IOException("No new leader founder for " + topic + " part "
          + partition);
    }

    boolean haveMoreMessages() throws IOException, InterruptedException {
      // we are done or already have more stuff queued up
      if (isDone || (pendingEvents != null && pendingEvents.hasNext())) {
        return !isDone;
      }
      int errorCount = 0;
      while (true) {
        FetchResponse response = consumer.fetch(new FetchRequestBuilder()
            .clientId(clientName)
            .addFetch(topic, partition, currentOffset, DATA_SIZE)
            .build());
        if (response.hasError()) {
          short code = response.errorCode(topic, partition);
          if (code == ErrorMapping.OffsetOutOfRangeCode()) {
            long oldOffset = currentOffset;
            currentOffset = getStartOffset();
            LOG.info("Jumping " + topic + " part " + partition + " from " +
                oldOffset + " to " + currentOffset);
            continue;
          }
          String errorMessage = "Error fetching data from broker: " +
              leader + " reason: " + code;
          if (++errorCount > ERROR_LIMIT) {
            throw new IOException(errorMessage);
          } else {
            LOG.warn(errorMessage);
          }
          consumer.close();
          findNewLeader();
          consumer = new SimpleConsumer(leader.host, leader.port, TIMEOUT,
              METADATA_BUFFER, clientName);
        } else {
          pendingEvents = response.messageSet(topic, partition).iterator();
          return pendingEvents.hasNext();
        }
      }
    }

    /**
     * Read the next block of events from Kafka and return it.
     * @return the next block of events or null if we've reached the front
     *    of the queue.
     * @throws IOException
     */
    Block next() throws IOException, InterruptedException {
      while (haveMoreMessages()) {
        MessageAndOffset next = pendingEvents.next();
        if (next.offset() >= currentOffset) {
          currentOffset = next.nextOffset();
          ByteBuffer msg = next.message().payload();
          Block result = Block.getRootAsBlock(msg, reusableObject);
          if (result != null) {
            long nextTime = result.events(reusableEvent, 0).time();
            if (nextTime >= fromDate) {
              fromDate = nextTime;
              if (nextTime > toDate) {
                break;
              }
              return result;
            }
          }
        }
      }
      // we are done, so close everything
      isDone = true;
      consumer.close();
      consumer = null;
      pendingEvents = null;
      return null;
    }

    private final String topic;
    private final int partition;
    private final String clientName;
    private final Set<HostPort> knownHosts = new HashSet<HostPort>();
    private long fromDate;
    private final long toDate;
    private final Block reusableObject = new Block();
    private final Event reusableEvent = new Event();

    private HostPort leader;
    private long currentOffset;
    private SimpleConsumer consumer;
    private Iterator<MessageAndOffset> pendingEvents = null;
    private boolean isDone = false;
  }

  static void readTopic(PrintStream out,
                        String brokers,
                        String topic,
                        String clientId,
                        long fromDate,
                        long toDate) throws IOException, InterruptedException {
    for(PartitionMetadata part: getMetadata(brokers, topic, clientId)) {
      out.println("topic = " + topic + ", part = " + part.partitionId()
           + ", leader = " + part.leader().host() + ":" + part.leader().port()
           + ", followers = " + (part.isr().size() - 1));
      RangeReader reader = new RangeReader(part, topic, fromDate, toDate,
          clientId);
      Block blk = reader.next();
      while (blk != null) {
        printBlock(out, blk);
        blk = reader.next();
      }
    }
  }

  static void parseDate(Calendar cal, String value) {
    String[] dateParts = value.split("-");
    if (dateParts.length != 3) {
      throw new IllegalArgumentException("Badly formed date '" + value + "'");
    }
    cal.set(Calendar.YEAR, Integer.parseInt(dateParts[0]));
    cal.set(Calendar.MONTH, Integer.parseInt(dateParts[1]) - 1);
    cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(dateParts[2]));
  }

  static void parseTime(Calendar cal, String value) {
    String[] timeParts = value.split(":");
    switch (timeParts.length) {
      case 3:
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(timeParts[0]));
        cal.set(Calendar.MINUTE, Integer.parseInt(timeParts[1]));
        cal.set(Calendar.SECOND, Integer.parseInt(timeParts[2]));
      case 2:
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(timeParts[0]));
        cal.set(Calendar.MINUTE, Integer.parseInt(timeParts[1]));
        cal.set(Calendar.SECOND, 0);
        break;
      case 1:
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(timeParts[0]));
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        break;
      default:
        throw new IllegalArgumentException("Too many time fields in '" + value +
            "'");
    }
  }

  static long parseDateTime(String value) {
    Calendar now = Calendar.getInstance();
    String[] dateTimeParts = value.split("\\+");
    if (dateTimeParts.length == 1) {
      parseTime(now, dateTimeParts[0]);
    } else if (dateTimeParts.length == 2) {
      parseDate(now, dateTimeParts[0]);
      parseTime(now, dateTimeParts[1]);
    } else {
      throw new IllegalArgumentException("Too many datetime fields in '" +
        value + "'");
    }
    return now.getTimeInMillis();
  }

  public static void main(String[] args
                          ) throws IOException, InterruptedException {
    String brokers = null;
    long fromDate = -2;
    long toDate = System.currentTimeMillis();
    for(int i=0; i < args.length; ++i) {
      if ("--broker-list".equals(args[i])) {
        brokers = args[++i];
      } else if ("--from".equals(args[i])) {
        fromDate = parseDateTime(args[++i]);
      } else if ("--to".equals(args[i])) {
        toDate = parseDateTime(args[++i]);
      } else if (brokers == null) {
        System.err.println("Must define the broker list using --broker-list");
      } else {
        readTopic(System.out, brokers, args[i], "ktoolog-cat", fromDate,
            toDate);
      }
    }
  }
}
