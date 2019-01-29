using System;

namespace VistaDB.Extra
{
  [Flags]
  internal enum TypeOfOperation
  {
    Insert = 1,
    Update = 16, // 0x00000010
    Delete = 256, // 0x00000100
    Nothing = 4096, // 0x00001000
  }
}
