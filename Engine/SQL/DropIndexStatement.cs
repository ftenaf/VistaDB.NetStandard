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
        ftsIndex = true;
        parser.SkipToken(true);
      }
      parser.SkipToken(true);
      if (ftsIndex && parser.IsToken("ON"))
      {
        indexName = (string) null;
      }
      else
      {
        indexName = parser.TokenValue.Token;
        parser.SkipToken(true);
      }
      parser.ExpectedExpression("ON");
      parser.SkipToken(true);
      tableName = parser.GetTableName((Statement) this);
      parser.SkipToken(false);
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      using (ITable table = (ITable) Database.OpenTable(tableName, false, false))
      {
        if (ftsIndex)
          table.DropFTSIndex();
        else
          table.DropIndex(indexName);
      }
      return (IQueryResult) null;
    }
  }
}
