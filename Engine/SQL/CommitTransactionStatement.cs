using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class CommitTransactionStatement : Statement
  {
    public CommitTransactionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      if (!parser.IsToken("TRANS"))
        parser.ExpectedExpression("TRANSACTION");
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (((ILocalSQLConnection) parent.Connection).ParentConnection.TransactionMode == VistaDBTransaction.TransactionMode.Ignore)
        return (IQueryResult) null;
      if (((ILocalSQLConnection) parent.Connection).ParentConnection.TransactionMode == VistaDBTransaction.TransactionMode.Off)
        throw new VistaDBException(460);
      parent.Connection.CommitTransaction();
      return (IQueryResult) null;
    }
  }
}
