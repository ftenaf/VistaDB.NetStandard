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
      statements.Add("FULLTEXT", indexStatementDescr);
      statements.Add("INDEX", indexStatementDescr);
      statements.Add("TABLE", new DropTableStatementDescr());
      statements.Add("VIEW", new DropViewStatementDescr());
      statements.Add("ASSEMBLY", new DropAssemblyStatementDescr());
      statements.Add("TRIGGER", new DropTriggerStatementDescr());
      IStatementDescr statementDescr = new DropProcStatementDescr();
      statements.Add("PROC", statementDescr);
      statements.Add("PROCEDURE", statementDescr);
      statements.Add("FUNCTION", new DropFunctionStatementDescr());
    }

    private class DropIndexStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropIndexStatement(conn, parent, parser, id);
      }
    }

    private class DropTableStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropTableStatement(conn, parent, parser, id);
      }
    }

    private class DropViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropViewStatement(conn, parent, parser, id);
      }
    }

    private class DropAssemblyStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropAssemblyStatement(conn, parent, parser, id);
      }
    }

    private class DropProcStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropProcStatement(conn, parent, parser, id);
      }
    }

    private class DropFunctionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropFunctionStatement(conn, parent, parser, id);
      }
    }

    private class DropTriggerStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new DropTriggerStatement(conn, parent, parser, id);
      }
    }
  }
}
