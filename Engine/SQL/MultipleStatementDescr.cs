using System.Collections;
using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal abstract class MultipleStatementDescr : IStatementDescr
  {
    protected IStatementDescr elseStatementDescr;
    protected Hashtable statements;

    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      parser.SkipToken(true);
      IStatementDescr statement = (IStatementDescr) this.statements[(object) parser.TokenValue.Token.ToUpper(CultureInfo.InvariantCulture)];
      if (statement != null)
        return statement.CreateStatement(conn, parent, parser, id);
      if (this.elseStatementDescr != null)
        return this.elseStatementDescr.CreateStatement(conn, parent, parser, id);
      string hint = "";
      foreach (string key in (IEnumerable) this.statements.Keys)
      {
        if (hint.Length != 0)
          hint += ", ";
        hint += key;
      }
      throw new VistaDBSQLException(507, hint, parser.TokenValue.RowNo, parser.TokenValue.ColNo);
    }
  }
}
