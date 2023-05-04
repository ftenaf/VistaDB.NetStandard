using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseCreateStatementDescr : MultipleStatementDescr
  {
    public BaseCreateStatementDescr()
    {
      statements = new Hashtable();
      IStatementDescr statementDescr1 = new CreateDatabaseStatementDescr();
      statements.Add("DATABASE", statementDescr1);
      statements.Add("INMEMORY", statementDescr1);
      statements.Add("TABLE", new CreateTableStatementDescr());
      statements.Add("VIEW", new CreateViewStatementDescr());
      statements.Add("ASSEMBLY", new CreateAssemblyStatementDescr());
      statements.Add("TRIGGER", new CreateTriggerStatementDescr());
      IStatementDescr statementDescr2 = new CreateProcStatementDescr();
      statements.Add("PROC", statementDescr2);
      statements.Add("PROCEDURE", statementDescr2);
      statements.Add("FUNCTION", new CreateFunctionStatementDescr());
      IStatementDescr statementDescr3 = new CreateIndexStatementDescr();
      statements.Add("INDEX", statementDescr3);
      statements.Add("UNIQUE", statementDescr3);
      statements.Add("CLUSTERED", statementDescr3);
      statements.Add("FULLTEXT", statementDescr3);
      statements.Add("NONCLUSTERED", statementDescr3);
    }

    private class CreateDatabaseStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateDatabaseStatement(conn, parent, parser, id);
      }
    }

    private class CreateTableStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateTableStatement(conn, parent, parser, id);
      }
    }

    private class CreateIndexStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateIndexStatement(conn, parent, parser, id);
      }
    }

    private class CreateViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateViewStatement(conn, parent, parser, id);
      }
    }

    private class CreateTriggerStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateTriggerStatement(conn, parent, parser, id);
      }
    }

    private class CreateAssemblyStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateAssemblyStatement(conn, parent, parser, id);
      }
    }

    private class CreateFunctionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateFunctionStatement(conn, parent, parser, id);
      }
    }

    private class CreateProcStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new CreateProcedureStatement(conn, parent, parser, id);
      }
    }
  }
}
