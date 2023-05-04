using System.Data;
using System.Data.Common;
using VistaDB.Diagnostic;

namespace VistaDB.Provider
{
  public class VistaDBTransaction : DbTransaction
  {
    private VistaDBConnection connection;

    internal VistaDBTransaction(VistaDBConnection connection)
    {
      this.connection = connection;
      this.connection.InternalBeginTransaction(this);
    }

    new public VistaDBConnection Connection
    {
      get
      {
        return connection;
      }
    }

    protected override DbConnection DbConnection
    {
      get
      {
        return Connection;
      }
    }

    public override IsolationLevel IsolationLevel
    {
      get
      {
        return IsolationLevel.ReadCommitted;
      }
    }

    public override void Commit()
    {
      if (connection.TransactionMode == TransactionMode.Off)
        throw new VistaDBException(460);
      if (connection.TransactionMode == TransactionMode.Ignore)
        return;
      connection.InternalCommitTransaction();
    }

    public override void Rollback()
    {
      connection.InternalRollbackTransaction();
    }

    protected override void Dispose(bool disposing)
    {
      connection = null;
      base.Dispose(disposing);
    }

    public enum TransactionMode
    {
      On,
      Off,
      Ignore,
    }
  }
}
