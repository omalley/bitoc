// automatically generated, do not modify

package org.apache.bitoc.flat;

import java.nio.*;
import java.lang.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ExceptionInfo extends Table {
  public static ExceptionInfo getRootAsExceptionInfo(ByteBuffer _bb) { return getRootAsExceptionInfo(_bb, new ExceptionInfo()); }
  public static ExceptionInfo getRootAsExceptionInfo(ByteBuffer _bb, ExceptionInfo obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public ExceptionInfo __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String text(int j) { int o = __offset(4); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int textLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createExceptionInfo(FlatBufferBuilder builder,
      int textOffset) {
    builder.startObject(1);
    ExceptionInfo.addText(builder, textOffset);
    return ExceptionInfo.endExceptionInfo(builder);
  }

  public static void startExceptionInfo(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addText(FlatBufferBuilder builder, int textOffset) { builder.addOffset(0, textOffset, 0); }
  public static int createTextVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startTextVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endExceptionInfo(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

