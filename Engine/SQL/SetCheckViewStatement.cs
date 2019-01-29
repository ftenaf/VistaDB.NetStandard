using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class SetCheckViewStatement : Statement
  {
    private bool checkView;

    public SetCheckViewStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      parser.ExpectedExpression("VIEW");
      parser.SkipToken(true);
      if (parser.IsToken("ON"))
      {
        this.checkView = true;
      }
      else
      {
        parser.ExpectedExpression("OFF");
        this.checkView = false;
      }
      parser.SkipToken(false);
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.connection.SetCheckView(this.checkView);
      return (IQueryResult) null;
    }
  }
}
