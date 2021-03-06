package org.example.pojo;


//public enum CmdResultCode{
//	Success,
//    TableAlreadyExist,	//client wants to create a new table but another table with the same name already exists
//    TableNotExist,	//client wants to drop a table but the table does not exist
//    IndexAlreadyExist,	//client wants to create a new index but another index with the same name already exists
//    TableNotExist2, //client wants to create a new index on a table but the table does not exist
//    IndexNotExist, //client wants to drop a index but the index does not exist
//    TableNotExist3, //client wants to drop a index but the index does not exist
//    UniqueValueAlreadyExist, //client wants to insert a record but the value of one of its unique columns already exists
//    TableNotExist4, //client wants to insert a record but the table does not exist
//    TableNotExist5, //client wants to delete some records but the table does not exist
//    InvalidCmdType,
//    TableNotHere,
//    UnknownError
//}

/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2021-06-05")
public enum CmdResultCode implements org.apache.thrift.TEnum {
  Success(0),
  TableAlreadyExist(1),
  TableNotExist(2),
  IndexAlreadyExist(3),
  TableNotExist2(4),
  IndexNotExist(5),
  TableNotExist3(6),
  UniqueValueAlreadyExist(7),
  TableNotExist4(8),
  TableNotExist5(9),
  InvalidCmdType(10),
  TableNotHere(11),
  UnknownError(12);

  private final int value;

  private CmdResultCode(int value) {
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
  public static CmdResultCode findByValue(int value) { 
    switch (value) {
      case 0:
        return Success;
      case 1:
        return TableAlreadyExist;
      case 2:
        return TableNotExist;
      case 3:
        return IndexAlreadyExist;
      case 4:
        return TableNotExist2;
      case 5:
        return IndexNotExist;
      case 6:
        return TableNotExist3;
      case 7:
        return UniqueValueAlreadyExist;
      case 8:
        return TableNotExist4;
      case 9:
        return TableNotExist5;
      case 10:
        return InvalidCmdType;
      case 11:
        return TableNotHere;
      case 12:
        return UnknownError;
      default:
        return null;
    }
  }
}
