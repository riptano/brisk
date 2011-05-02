/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.thriftfs.jobtracker.api;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

/**
 * Enum version of the ints in JobStatus
 */
public enum ThriftJobState implements org.apache.thrift.TEnum {
  RUNNING(1),
  SUCCEEDED(2),
  FAILED(3),
  PREP(4),
  KILLED(5);

  private final int value;

  private ThriftJobState(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static ThriftJobState findByValue(int value) { 
    switch (value) {
      case 1:
        return RUNNING;
      case 2:
        return SUCCEEDED;
      case 3:
        return FAILED;
      case 4:
        return PREP;
      case 5:
        return KILLED;
      default:
        return null;
    }
  }
}
