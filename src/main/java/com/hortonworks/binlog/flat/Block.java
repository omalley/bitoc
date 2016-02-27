// automatically generated, do not modify

package com.hortonworks.binlog.flat;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Block extends Table {
  public static Block getRootAsBlock(ByteBuffer _bb) { return getRootAsBlock(_bb, new Block()); }
  public static Block getRootAsBlock(ByteBuffer _bb, Block obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public Block __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public Event events(int j) { return events(new Event(), j); }
  public Event events(Event obj, int j) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int eventsLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createBlock(FlatBufferBuilder builder,
      int eventsOffset) {
    builder.startObject(1);
    Block.addEvents(builder, eventsOffset);
    return Block.endBlock(builder);
  }

  public static void startBlock(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addEvents(FlatBufferBuilder builder, int eventsOffset) { builder.addOffset(0, eventsOffset, 0); }
  public static int createEventsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startEventsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endBlock(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishBlockBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};

