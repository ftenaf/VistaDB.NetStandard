using System.Collections;
using System.Globalization;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseSetStatementDescr : IStatementDescr
  {
    private Hashtable statements;

    public BaseSetStatementDescr()
    {
      statements = new Hashtable();
      statements.Add("OPTIMIZATION", new SetOptimizationStatementDescr());
      statements.Add("CHECK", new SetCheckViewStatementDescr());
      statements.Add("GROUP", new SetGroupOptimizationStatementDescr());
      statements.Add("SYNCHRONIZATION", new SetGroupSynchronizationStatementDescr());
    }

    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      parser.SkipToken(true);
      IStatementDescr statement = (IStatementDescr) statements[parser.TokenValue.Token.ToUpper(CultureInfo.InvariantCulture)];
      if (statement == null)
        return new SetVariableStatement(conn, parent, parser, id);
      return statement.CreateStatement(conn, parent, parser, id);
    }

    internal class SetOptimizationStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new SetOptimizationStatement(conn, parent, parser, id);
      }
    }

    internal class SetCheckViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new SetCheckViewStatement(conn, parent, parser, id);
      }
    }

    internal class SetGroupOptimizationStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new SetGroupOptimizationStatement(conn, parent, parser, id);
      }
    }

    internal class SetGroupSynchronizationStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new SetGroupSynchronizationStatement(conn, parent, parser, id);
      }
    }
  }
}
