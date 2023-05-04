using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseCreateStatementDescr : MultipleStatementDescr
  {
    public BaseCreateStatementDescr()
    {
      statements = new Hashtable();
      IStatementDescr statementDescr1 = (IStatementDescr) new CreateDatabaseStatementDescr();
      statements.Add((object) "DATABASE", (object) statementDescr1);
      statements.Add((object) "INMEMORY", (object) statementDescr1);
      statements.Add((object) "TABLE", (object) new CreateTableStatementDescr());
      statements.Add((object) "VIEW", (object) new CreateViewStatementDescr());
      statements.Add((object) "ASSEMBLY", (object) new CreateAssemblyStatementDescr());
      statements.Add((object) "TRIGGER", (object) new CreateTriggerStatementDescr());
      IStatementDescr statementDescr2 = (IStatementDescr) new CreateProcStatementDescr();
      statements.Add((object) "PROC", (object) statementDescr2);
      statements.Add((object) "PROCEDURE", (object) statementDescr2);
      statements.Add((object) "FUNCTION", (object) new CreateFunctionStatementDescr());
      IStatementDescr statementDescr3 = (IStatementDescr) new CreateIndexStatementDescr();
      statements.Add((object) "INDEX", (object) statementDescr3);
      statements.Add((object) "UNIQUE", (object) statementDescr3);
      statements.Add((object) "CLUSTERED", (object) statementDescr3);
      statements.Add((object) "FULLTEXT", (object) statementDescr3);
      statements.Add((object) "NONCLUSTERED", (object) statementDescr3);
    }

    private class CreateDatabaseStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateDatabaseStatement(conn, parent, parser, id);
      }
    }

    private class CreateTableStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateTableStatement(conn, parent, parser, id);
      }
    }

    private class CreateIndexStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateIndexStatement(conn, parent, parser, id);
      }
    }

    private class CreateViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateViewStatement(conn, parent, parser, id);
      }
    }

    private class CreateTriggerStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateTriggerStatement(conn, parent, parser, id);
      }
    }

    private class CreateAssemblyStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateAssemblyStatement(conn, parent, parser, id);
      }
    }

    private class CreateFunctionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateFunctionStatement(conn, parent, parser, id);
      }
    }

    private class CreateProcStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new CreateProcedureStatement(conn, parent, parser, id);
      }
    }
  }
}
