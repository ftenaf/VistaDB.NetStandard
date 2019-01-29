using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropIndexStatement : BaseCreateStatement
  {
    private string indexName;
    private string tableName;
    private bool ftsIndex;

    public DropIndexStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (parser.IsToken("FULLTEXT"))
      {
        this.ftsIndex = true;
        parser.SkipToken(true);
      }
      parser.SkipToken(true);
      if (this.ftsIndex && parser.IsToken("ON"))
      {
        this.indexName = (string) null;
      }
      else
      {
        this.indexName = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      this.tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(false);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      using (ITable table = (ITable) this.Database.OpenTable(this.tableName, false, false))
      {
        if (this.ftsIndex)
          table.DropFTSIndex();
        else
          table.DropIndex(this.indexName);
      }
      return (IQueryResult) null;
    }
  }
}
