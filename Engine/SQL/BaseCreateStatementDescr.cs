using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseCreateStatementDescr : MultipleStatementDescr
  {
    public BaseCreateStatementDescr()
    {
      this.statements = new Hashtable();
      IStatementDescr statementDescr1 = (IStatementDescr) new BaseCreateStatementDescr.CreateDatabaseStatementDescr();
      this.statements.Add((object) "DATABASE", (object) statementDescr1);
      this.statements.Add((object) "INMEMORY", (object) statementDescr1);
      this.statements.Add((object) "TABLE", (object) new BaseCreateStatementDescr.CreateTableStatementDescr());
      this.statements.Add((object) "VIEW", (object) new BaseCreateStatementDescr.CreateViewStatementDescr());
      this.statements.Add((object) "ASSEMBLY", (object) new BaseCreateStatementDescr.CreateAssemblyStatementDescr());
      this.statements.Add((object) "TRIGGER", (object) new BaseCreateStatementDescr.CreateTriggerStatementDescr());
      IStatementDescr statementDescr2 = (IStatementDescr) new BaseCreateStatementDescr.CreateProcStatementDescr();
      this.statements.Add((object) "PROC", (object) statementDescr2);
      this.statements.Add((object) "PROCEDURE", (object) statementDescr2);
      this.statements.Add((object) "FUNCTION", (object) new BaseCreateStatementDescr.CreateFunctionStatementDescr());
      IStatementDescr statementDescr3 = (IStatementDescr) new BaseCreateStatementDescr.CreateIndexStatementDescr();
      this.statements.Add((object) "INDEX", (object) statementDescr3);
      this.statements.Add((object) "UNIQUE", (object) statementDescr3);
      this.statements.Add((object) "CLUSTERED", (object) statementDescr3);
      this.statements.Add((object) "FULLTEXT", (object) statementDescr3);
      this.statements.Add((object) "NONCLUSTERED", (object) statementDescr3);
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
