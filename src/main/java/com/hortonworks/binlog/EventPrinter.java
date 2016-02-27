package com.hortonworks.binlog;

import com.hortonworks.binlog.flat.Block;
import com.hortonworks.binlog.flat.Event;
import com.hortonworks.binlog.flat.ExceptionInfo;
import com.hortonworks.binlog.flat.Level;
import com.hortonworks.binlog.flat.Location;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Date;

/**
 * Print the serialized event blobs.
 */
public class EventPrinter {

  public static void printFile(PrintStream out,
                               String filename) throws IOException {
    File file = new File(filename);
    RandomAccessFile f = new RandomAccessFile(file, "r");
    byte[] buffer = new byte[(int)f.length()];
    f.readFully(buffer);
    f.close();
    Block eventBlock = Block.getRootAsBlock(ByteBuffer.wrap(buffer));
    Event staticEvent = new Event();
    Location staticLocation = new Location();
    ExceptionInfo staticException = new ExceptionInfo();
    int eventCount = eventBlock.eventsLength();
    for(int i=0; i < eventCount; ++i) {
      Event event = eventBlock.events(staticEvent, i);
      out.print("{\"time\": \"");
      out.print(new Date(event.time()));
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
        out.print("\", \"line\": \"");
        out.print(location.line());
        out.print("\"}, ");
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

  public static void main(String[] args) throws IOException {
    for(int i=0; i < args.length; ++i) {
      printFile(System.out, args[i]);
    }
  }
}
