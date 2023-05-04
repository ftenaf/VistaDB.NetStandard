using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class RollbackTransactionStatement : Statement
  {
    public RollbackTransactionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
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
      parent.Connection.RollbackTransaction();
      return (IQueryResult) null;
    }
  }
}
