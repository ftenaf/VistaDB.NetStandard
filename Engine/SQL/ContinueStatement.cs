using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class ContinueStatement : Statement
  {
    internal ContinueStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
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
      WhileStatement cycleStatement = this.DoGetCycleStatement();
      if (cycleStatement == null)
        throw new VistaDBSQLException(643, (string) null, this.lineNo, this.symbolNo);
      cycleStatement.Continue();
      return (IQueryResult) null;
    }
  }
}
