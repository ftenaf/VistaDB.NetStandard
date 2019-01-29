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
      this.statements = new Hashtable();
      this.statements.Add((object) "OPTIMIZATION", (object) new BaseSetStatementDescr.SetOptimizationStatementDescr());
      this.statements.Add((object) "CHECK", (object) new BaseSetStatementDescr.SetCheckViewStatementDescr());
      this.statements.Add((object) "GROUP", (object) new BaseSetStatementDescr.SetGroupOptimizationStatementDescr());
      this.statements.Add((object) "SYNCHRONIZATION", (object) new BaseSetStatementDescr.SetGroupSynchronizationStatementDescr());
    }

    public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
    {
      parser.SkipToken(true);
      IStatementDescr statement = (IStatementDescr) this.statements[(object) parser.TokenValue.Token.ToUpper(CultureInfo.InvariantCulture)];
      if (statement == null)
        return (Statement) new SetVariableStatement(conn, parent, parser, id);
      return statement.CreateStatement(conn, parent, parser, id);
    }

    internal class SetOptimizationStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new SetOptimizationStatement(conn, parent, parser, id);
      }
    }

    internal class SetCheckViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new SetCheckViewStatement(conn, parent, parser, id);
      }
    }

    internal class SetGroupOptimizationStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new SetGroupOptimizationStatement(conn, parent, parser, id);
      }
    }

    internal class SetGroupSynchronizationStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new SetGroupSynchronizationStatement(conn, parent, parser, id);
      }
    }
  }
}
