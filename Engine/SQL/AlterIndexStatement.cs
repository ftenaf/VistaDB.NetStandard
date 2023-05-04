using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class AlterIndexStatement : BaseCreateStatement
  {
    private string indexName;
    private string tableName;

    public AlterIndexStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      indexName = !parser.IsToken("ALL") || parser.TokenValue.TokenType != TokenType.Unknown ? parser.TokenValue.Token : null;
      parser.SkipToken(true);
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      tableName = parser.GetTableName(this);
      parser.SkipToken(true);
      if (parser.IsToken("REBUILD"))
        throw new VistaDBSQLException(509, "Rebuilding Indexes is not supported. Packing a database rebuilds them automatically.", lineNo, symbolNo);
      parser.ExpectedExpression("REBUILD");
      parser.SkipToken(false);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      throw new VistaDBSQLException(509, "Alter Index is not implemented at this time.", lineNo, symbolNo);
    }
  }
}
