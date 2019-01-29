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
      this.name = parser.TokenValue.Token;
      parser.SkipToken(true);
      if (parser.IsToken("DESCRIPTION"))
      {
        parser.SkipToken(true);
        this.description = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      else
        this.description = (string) null;
      parser.ExpectedExpression("FROM");
      parser.SkipToken(true);
      this.fileName = parser.TokenValue.Token;
      parser.SkipToken(false);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.Database.AddAssembly(this.name, this.fileName, this.description);
      return (IQueryResult) null;
    }
  }
}
