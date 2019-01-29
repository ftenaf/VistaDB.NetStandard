namespace VistaDB
{
  public enum VistaDBType
  {
    Uninitialized = -1,
    Char = 1,
    NChar = 2,
    VarChar = 3,
    NVarChar = 4,
    Text = 5,
    NText = 6,
    TinyInt = 8,
    SmallInt = 9,
    Int = 10, // 0x0000000A
    BigInt = 11, // 0x0000000B
    Real = 12, // 0x0000000C
    Float = 13, // 0x0000000D
    Decimal = 14, // 0x0000000E
    Money = 15, // 0x0000000F
    SmallMoney = 16, // 0x00000010
    Bit = 17, // 0x00000011
    DateTime = 19, // 0x00000013
    Image = 20, // 0x00000014
    VarBinary = 21, // 0x00000015
    UniqueIdentifier = 22, // 0x00000016
    SmallDateTime = 23, // 0x00000017
    Timestamp = 24, // 0x00000018
    Unknown = 31, // 0x0000001F
  }
}
