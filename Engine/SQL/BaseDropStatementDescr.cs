using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseDropStatementDescr : MultipleStatementDescr
  {
    public BaseDropStatementDescr()
    {
      statements = new Hashtable();
            DropIndexStatementDescr indexStatementDescr = new DropIndexStatementDescr();
      statements.Add((object) "FULLTEXT", (object) indexStatementDescr);
      statements.Add((object) "INDEX", (object) indexStatementDescr);
      statements.Add((object) "TABLE", (object) new DropTableStatementDescr());
      statements.Add((object) "VIEW", (object) new DropViewStatementDescr());
      statements.Add((object) "ASSEMBLY", (object) new DropAssemblyStatementDescr());
      statements.Add((object) "TRIGGER", (object) new DropTriggerStatementDescr());
      IStatementDescr statementDescr = (IStatementDescr) new DropProcStatementDescr();
      statements.Add((object) "PROC", (object) statementDescr);
      statements.Add((object) "PROCEDURE", (object) statementDescr);
      statements.Add((object) "FUNCTION", (object) new DropFunctionStatementDescr());
    }

    private class DropIndexStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropIndexStatement(conn, parent, parser, id);
      }
    }

    private class DropTableStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropTableStatement(conn, parent, parser, id);
      }
    }

    private class DropViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropViewStatement(conn, parent, parser, id);
      }
    }

    private class DropAssemblyStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropAssemblyStatement(conn, parent, parser, id);
      }
    }

    private class DropProcStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropProcStatement(conn, parent, parser, id);
      }
    }

    private class DropFunctionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropFunctionStatement(conn, parent, parser, id);
      }
    }

    private class DropTriggerStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new DropTriggerStatement(conn, parent, parser, id);
      }
    }
  }
}
