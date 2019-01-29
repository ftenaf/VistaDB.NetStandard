using System;

namespace VistaDB.Engine.Core
{
  [Flags]
  internal enum LicenseKeyFlags
  {
    None = 0,
    Production = 1,
    Trial = 2,
    Expiring = 4,
  }
}
