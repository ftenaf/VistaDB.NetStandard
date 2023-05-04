using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseAlterStatementDescr : MultipleStatementDescr
  {
    public BaseAlterStatementDescr()
    {
      statements = new Hashtable();
      statements.Add("VIEW", new AlterViewStatementDescr());
      statements.Add("ASSEMBLY", new AlterAssemblyStatementDescr());
      IStatementDescr statementDescr = new AlterProcedureStatementDescr();
      statements.Add("PROCEDURE", statementDescr);
      statements.Add("PROC", statementDescr);
      statements.Add("FUNCTION", new AlterFunctionStatementDescr());
      statements.Add("TABLE", new AlterTableStatementDescr());
      statements.Add("INDEX", new AlterIndexStatementDescr());
    }

    private class AlterViewStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new AlterViewStatement(conn, parent, parser, id);
      }
    }

    private class AlterAssemblyStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new AlterAssemblyStatement(conn, parent, parser, id);
      }
    }

    private class AlterFunctionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new AlterFunctionStatement(conn, parent, parser, id);
      }
    }

    private class AlterProcedureStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new AlterProcedureStatement(conn, parent, parser, id);
      }
    }

    private class AlterTableStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new AlterTableStatement(conn, parent, parser, id);
      }
    }

    private class AlterIndexStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return new AlterIndexStatement(conn, parent, parser, id);
      }
    }
  }
}
