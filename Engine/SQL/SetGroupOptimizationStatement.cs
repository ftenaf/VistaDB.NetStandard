using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class SetGroupOptimizationStatement : Statement
  {
    private bool optimization;

    public SetGroupOptimizationStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      parser.ExpectedExpression("OPTIMIZATION");
      parser.SkipToken(true);
      if (parser.IsToken("ON"))
      {
        this.optimization = true;
      }
      else
      {
        parser.ExpectedExpression("OFF");
        this.optimization = false;
      }
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.connection.SetGroupOptimization(this.optimization);
      return (IQueryResult) null;
    }
  }
}
