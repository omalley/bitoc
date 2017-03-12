// automatically generated, do not modify

package com.hortonworks.ktoolog.flat;

public final class Level {
  private Level() { }
  public static final byte FATAL = 0;
  public static final byte ERROR = 1;
  public static final byte WARN = 2;
  public static final byte INFO = 3;
  public static final byte DEBUG = 4;
  public static final byte TRACE = 5;

  private static final String[] names = { "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", };

  public static String name(int e) { return names[e]; }
};

