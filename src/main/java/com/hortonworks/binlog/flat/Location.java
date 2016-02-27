// automatically generated, do not modify

package com.hortonworks.binlog.flat;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Location extends Table {
  public static Location getRootAsLocation(ByteBuffer _bb) { return getRootAsLocation(_bb, new Location()); }
  public static Location getRootAsLocation(ByteBuffer _bb, Location obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public Location __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String className() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer classNameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public String fileName() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer fileNameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public String methodName() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer methodNameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public String line() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lineAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }

  public static int createLocation(FlatBufferBuilder builder,
      int classNameOffset,
      int fileNameOffset,
      int methodNameOffset,
      int lineOffset) {
    builder.startObject(4);
    Location.addLine(builder, lineOffset);
    Location.addMethodName(builder, methodNameOffset);
    Location.addFileName(builder, fileNameOffset);
    Location.addClassName(builder, classNameOffset);
    return Location.endLocation(builder);
  }

  public static void startLocation(FlatBufferBuilder builder) { builder.startObject(4); }
  public static void addClassName(FlatBufferBuilder builder, int classNameOffset) { builder.addOffset(0, classNameOffset, 0); }
  public static void addFileName(FlatBufferBuilder builder, int fileNameOffset) { builder.addOffset(1, fileNameOffset, 0); }
  public static void addMethodName(FlatBufferBuilder builder, int methodNameOffset) { builder.addOffset(2, methodNameOffset, 0); }
  public static void addLine(FlatBufferBuilder builder, int lineOffset) { builder.addOffset(3, lineOffset, 0); }
  public static int endLocation(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

