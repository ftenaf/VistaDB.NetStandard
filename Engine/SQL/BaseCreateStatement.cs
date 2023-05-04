using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal abstract class BaseCreateStatement : Statement
  {
    protected BaseCreateStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void DoBeforeParse()
    {
      hasDDL = true;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      connection.CloseAllPooledTables();
      return null;
    }

    protected static int StrTokenToInt(SQLParser parser)
    {
      switch (parser.TokenValue.TokenType)
      {
        case TokenType.Integer:
          return int.Parse(parser.TokenValue.Token, CrossConversion.NumberFormat);
        case TokenType.Float:
          return (int) double.Parse(parser.TokenValue.Token, CrossConversion.NumberFormat);
        default:
          throw new VistaDBSQLException(507, "numeric constant", parser.TokenValue.RowNo, parser.TokenValue.ColNo);
      }
    }
  }
}
