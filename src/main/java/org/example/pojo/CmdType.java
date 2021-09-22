package org.example.pojo;

/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2021-06-05")
public enum CmdType implements org.apache.thrift.TEnum {
  CREATE_TABLE(0),
  DROP_TABLE(1),
  CREATE_INDEX(2),
  DROP_INDEX(3),
  INSERT(4),
  DELETE(5),
  SELECT(6),
  Nofunc(7),
  SyntaxError(8);

  private final int value;

  private CmdType(int value) {
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
  @org.apache.thrift.annotation.Nullable
  public static CmdType findByValue(int value) { 
    switch (value) {
      case 0:
        return CREATE_TABLE;
      case 1:
        return DROP_TABLE;
      case 2:
        return CREATE_INDEX;
      case 3:
        return DROP_INDEX;
      case 4:
        return INSERT;
      case 5:
        return DELETE;
      default:
        return null;
    }
  }
}