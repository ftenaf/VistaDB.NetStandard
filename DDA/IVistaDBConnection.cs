using System;

namespace VistaDB.DDA
{
  public interface IVistaDBConnection : IDisposable
  {
    long Id { get; }

    int LCID { get; set; }

    int PageSize { get; set; }

    int LockTimeout { get; set; }

    bool PersistentLockFiles { get; set; }
  }
}
