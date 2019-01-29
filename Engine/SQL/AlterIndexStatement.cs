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
      this.indexName = !parser.IsToken("ALL") || parser.TokenValue.TokenType != TokenType.Unknown ? parser.TokenValue.Token : (string) null;
      parser.SkipToken(true);
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      this.tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(true);
      if (parser.IsToken("REBUILD"))
        throw new VistaDBSQLException(509, "Rebuilding Indexes is not supported. Packing a database rebuilds them automatically.", this.lineNo, this.symbolNo);
      parser.ExpectedExpression("REBUILD");
      parser.SkipToken(false);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      throw new VistaDBSQLException(509, "Alter Index is not implemented at this time.", this.lineNo, this.symbolNo);
    }
  }
}
