using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BeginTransactionStatement : Statement
  {
    public BeginTransactionStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      parent.Connection.BeginTransaction();
      return (IQueryResult) null;
    }
  }
}
