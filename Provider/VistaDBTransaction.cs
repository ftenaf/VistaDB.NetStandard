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

    public VistaDBConnection Connection
    {
      get
      {
        return this.connection;
      }
    }

    protected override DbConnection DbConnection
    {
      get
      {
        return (DbConnection) this.Connection;
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
      if (this.connection.TransactionMode == VistaDBTransaction.TransactionMode.Off)
        throw new VistaDBException(460);
      if (this.connection.TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
        return;
      this.connection.InternalCommitTransaction();
    }

    public override void Rollback()
    {
      this.connection.InternalRollbackTransaction();
    }

    protected override void Dispose(bool disposing)
    {
      this.connection = (VistaDBConnection) null;
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
