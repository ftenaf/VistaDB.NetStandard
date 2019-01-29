using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class DropTableStatement : BaseCreateStatement
  {
    protected List<string> tableNames = new List<string>();

    public DropTableStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      do
      {
        parser.SkipToken(true);
        this.tableNames.Add(parser.GetTableName((Statement) this));
        parser.SkipToken(false);
      }
      while (parser.IsToken(","));
    }

    protected override IQueryResult OnExecuteQuery()
    {
      base.OnExecuteQuery();
      foreach (string tableName in this.tableNames)
        this.Database.DropTable(tableName);
      return (IQueryResult) null;
    }
  }
}
