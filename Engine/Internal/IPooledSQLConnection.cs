using System;
using System.Data.Common;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IPooledSQLConnection : IVistaDBConnection, IDisposable
  {
    void PrepareConnectionForPool();

    void InitializeConnectionFromPool(DbConnection parentConnection);
  }
}
