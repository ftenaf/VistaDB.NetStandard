using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class TruncateTableStatement : DeleteStatement
  {
    internal TruncateTableStatement(LocalSQLConnection localSqlConnection, Statement parent, SQLParser parser, long id)
      : base(localSqlConnection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      parser.ExpectedExpression("TABLE", null);
      base.OnParse(connection, parser);
    }
  }
}
