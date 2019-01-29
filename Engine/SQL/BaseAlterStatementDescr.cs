using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseAlterStatementDescr : MultipleStatementDescr
  {
    public BaseAlterStatementDescr()
    {
      this.statements = new Hashtable();
      this.statements.Add((object) "VIEW", (object) new BaseAlterStatementDescr.AlterViewStatementDescr());
      this.statements.Add((object) "ASSEMBLY", (object) new BaseAlterStatementDescr.AlterAssemblyStatementDescr());
      IStatementDescr statementDescr = (IStatementDescr) new BaseAlterStatementDescr.AlterProcedureStatementDescr();
      this.statements.Add((object) "PROCEDURE", (object) statementDescr);
      this.statements.Add((object) "PROC", (object) statementDescr);
      this.statements.Add((object) "FUNCTION", (object) new BaseAlterStatementDescr.AlterFunctionStatementDescr());
      this.statements.Add((object) "TABLE", (object) new BaseAlterStatementDescr.AlterTableStatementDescr());
      this.statements.Add((object) "INDEX", (object) new BaseAlterStatementDescr.AlterIndexStatementDescr());
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
