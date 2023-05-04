using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseAlterStatementDescr : MultipleStatementDescr
  {
    public BaseAlterStatementDescr()
    {
      statements = new Hashtable();
      statements.Add((object) "VIEW", (object) new AlterViewStatementDescr());
      statements.Add((object) "ASSEMBLY", (object) new AlterAssemblyStatementDescr());
      IStatementDescr statementDescr = (IStatementDescr) new AlterProcedureStatementDescr();
      statements.Add((object) "PROCEDURE", (object) statementDescr);
      statements.Add((object) "PROC", (object) statementDescr);
      statements.Add((object) "FUNCTION", (object) new AlterFunctionStatementDescr());
      statements.Add((object) "TABLE", (object) new AlterTableStatementDescr());
      statements.Add((object) "INDEX", (object) new AlterIndexStatementDescr());
    }

    private class AlterViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new AlterViewStatement(conn, parent, parser, id);
      }
    }

    private class AlterAssemblyStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new AlterAssemblyStatement(conn, parent, parser, id);
      }
    }

    private class AlterFunctionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new AlterFunctionStatement(conn, parent, parser, id);
      }
    }

    private class AlterProcedureStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new AlterProcedureStatement(conn, parent, parser, id);
      }
    }

    private class AlterTableStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new AlterTableStatement(conn, parent, parser, id);
      }
    }

    private class AlterIndexStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new AlterIndexStatement(conn, parent, parser, id);
      }
    }
  }
}
