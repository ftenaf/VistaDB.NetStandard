using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class CreateAssemblyStatement : BaseCreateStatement
  {
    protected string name;
    protected string description;
    protected string fileName;

    public CreateAssemblyStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      name = parser.TokenValue.Token;
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        description = null;
      parser.ExpectedExpression("FROM");
      parser.SkipToken(true);
      fileName = parser.TokenValue.Token;
      parser.SkipToken(false);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      Database.AddAssembly(name, fileName, description);
      return null;
    }
  }
}
