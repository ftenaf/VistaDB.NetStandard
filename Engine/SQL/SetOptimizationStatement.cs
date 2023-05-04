using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class SetOptimizationStatement : Statement
  {
    private bool optimization;

    public SetOptimizationStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      if (parser.IsToken("ON"))
      {
        optimization = true;
      }
      else
      {
        parser.ExpectedExpression("OFF", "ON");
        optimization = false;
      }
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      connection.SetOptimization(optimization);
      return (IQueryResult) null;
    }
  }
}
