using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseDropStatementDescr : MultipleStatementDescr
  {
    public BaseDropStatementDescr()
    {
      this.statements = new Hashtable();
      BaseDropStatementDescr.DropIndexStatementDescr indexStatementDescr = new BaseDropStatementDescr.DropIndexStatementDescr();
      this.statements.Add((object) "FULLTEXT", (object) indexStatementDescr);
      this.statements.Add((object) "INDEX", (object) indexStatementDescr);
      this.statements.Add((object) "TABLE", (object) new BaseDropStatementDescr.DropTableStatementDescr());
      this.statements.Add((object) "VIEW", (object) new BaseDropStatementDescr.DropViewStatementDescr());
      this.statements.Add((object) "ASSEMBLY", (object) new BaseDropStatementDescr.DropAssemblyStatementDescr());
      this.statements.Add((object) "TRIGGER", (object) new BaseDropStatementDescr.DropTriggerStatementDescr());
      IStatementDescr statementDescr = (IStatementDescr) new BaseDropStatementDescr.DropProcStatementDescr();
      this.statements.Add((object) "PROC", (object) statementDescr);
      this.statements.Add((object) "PROCEDURE", (object) statementDescr);
      this.statements.Add((object) "FUNCTION", (object) new BaseDropStatementDescr.DropFunctionStatementDescr());
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
