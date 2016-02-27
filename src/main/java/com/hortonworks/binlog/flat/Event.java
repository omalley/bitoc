// automatically generated, do not modify

package com.hortonworks.binlog.flat;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Event extends Table {
  public static Event getRootAsEvent(ByteBuffer _bb) { return getRootAsEvent(_bb, new Event()); }
  public static Event getRootAsEvent(ByteBuffer _bb, Event obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public Event __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public long time() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public byte level() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) : 3; }
  public String message() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer messageAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ExceptionInfo throwable() { return throwable(new ExceptionInfo()); }
  public ExceptionInfo throwable(ExceptionInfo obj) { int o = __offset(10); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public String thread() { int o = __offset(12); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer threadAsByteBuffer() { return __vector_as_bytebuffer(12, 1); }
  public String logger() { int o = __offset(14); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer loggerAsByteBuffer() { return __vector_as_bytebuffer(14, 1); }
  public Location location() { return location(new Location()); }
  public Location location(Location obj) { int o = __offset(16); return o != 0 ? obj.__init(__indirect(o + bb_pos), bb) : null; }
  public String project() { int o = __offset(18); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer projectAsByteBuffer() { return __vector_as_bytebuffer(18, 1); }
  public String server() { int o = __offset(20); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer serverAsByteBuffer() { return __vector_as_bytebuffer(20, 1); }
  public String hostName() { int o = __offset(22); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer hostNameAsByteBuffer() { return __vector_as_bytebuffer(22, 1); }
  public String ipAddress() { int o = __offset(24); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer ipAddressAsByteBuffer() { return __vector_as_bytebuffer(24, 1); }
  public int pid() { int o = __offset(26); return o != 0 ? bb.getInt(o + bb_pos) : 0; }

  public static int createEvent(FlatBufferBuilder builder,
      long time,
      byte level,
      int messageOffset,
      int throwableOffset,
      int threadOffset,
      int loggerOffset,
      int locationOffset,
      int projectOffset,
      int serverOffset,
      int hostNameOffset,
      int ipAddressOffset,
      int pid) {
    builder.startObject(12);
    Event.addTime(builder, time);
    Event.addPid(builder, pid);
    Event.addIpAddress(builder, ipAddressOffset);
    Event.addHostName(builder, hostNameOffset);
    Event.addServer(builder, serverOffset);
    Event.addProject(builder, projectOffset);
    Event.addLocation(builder, locationOffset);
    Event.addLogger(builder, loggerOffset);
    Event.addThread(builder, threadOffset);
    Event.addThrowable(builder, throwableOffset);
    Event.addMessage(builder, messageOffset);
    Event.addLevel(builder, level);
    return Event.endEvent(builder);
  }

  public static void startEvent(FlatBufferBuilder builder) { builder.startObject(12); }
  public static void addTime(FlatBufferBuilder builder, long time) { builder.addLong(0, time, 0); }
  public static void addLevel(FlatBufferBuilder builder, byte level) { builder.addByte(1, level, 3); }
  public static void addMessage(FlatBufferBuilder builder, int messageOffset) { builder.addOffset(2, messageOffset, 0); }
  public static void addThrowable(FlatBufferBuilder builder, int throwableOffset) { builder.addOffset(3, throwableOffset, 0); }
  public static void addThread(FlatBufferBuilder builder, int threadOffset) { builder.addOffset(4, threadOffset, 0); }
  public static void addLogger(FlatBufferBuilder builder, int loggerOffset) { builder.addOffset(5, loggerOffset, 0); }
  public static void addLocation(FlatBufferBuilder builder, int locationOffset) { builder.addOffset(6, locationOffset, 0); }
  public static void addProject(FlatBufferBuilder builder, int projectOffset) { builder.addOffset(7, projectOffset, 0); }
  public static void addServer(FlatBufferBuilder builder, int serverOffset) { builder.addOffset(8, serverOffset, 0); }
  public static void addHostName(FlatBufferBuilder builder, int hostNameOffset) { builder.addOffset(9, hostNameOffset, 0); }
  public static void addIpAddress(FlatBufferBuilder builder, int ipAddressOffset) { builder.addOffset(10, ipAddressOffset, 0); }
  public static void addPid(FlatBufferBuilder builder, int pid) { builder.addInt(11, pid, 0); }
  public static int endEvent(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

