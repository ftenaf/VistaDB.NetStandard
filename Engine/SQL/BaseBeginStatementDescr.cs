using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class BaseBeginStatementDescr : MultipleStatementDescr
  {
    public BaseBeginStatementDescr()
    {
      IStatementDescr statementDescr = (IStatementDescr) new BeginTransactionStatementDescr();
      statements = new Hashtable();
      statements.Add((object) "TRANS", (object) statementDescr);
      statements.Add((object) "TRANSACTION", (object) statementDescr);
      statements.Add((object) "TRY", (object) new BeginTryBlockStatementDescr());
      elseStatementDescr = (IStatementDescr) new BeginBlockStatementDescr();
    }

    private class BeginBlockStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        BatchStatement batchStatement;
        switch (parser.Context.ContextType)
        {
          case CurrentTokenContext.TokenContext.StoredProcedure:
            batchStatement = (BatchStatement) new StoredProcedureBody(conn, parent, parser, id);
            break;
          case CurrentTokenContext.TokenContext.StoredFunction:
            batchStatement = (BatchStatement) new StoredFunctionBody(conn, parent, parser, id);
            break;
          default:
            batchStatement = new BatchStatement(conn, parent, parser, id);
            break;
        }
        parser.PushContext(new CurrentTokenContext(CurrentTokenContext.TokenContext.UsualText, string.Empty));
        try
        {
          while (!parser.SkipSemicolons())
          {
            if (!parser.IsToken("END"))
              batchStatement.Add(conn.ParseStatement((Statement) batchStatement, id));
            else
              break;
          }
        }
        finally
        {
          parser.PopContext();
        }
        parser.ExpectedExpression("END");
        parser.SkipToken(false);
        return (Statement) batchStatement;
      }
    }

    private class BeginTransactionStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        return (Statement) new BeginTransactionStatement(conn, parent, parser, id);
      }
    }

    private class BeginTryBlockStatementDescr : IStatementDescr
    {
      public Statement CreateStatement(LocalSQLConnection conn, Statement parent, SQLParser parser, long id)
      {
        TryBlockStatement tryBlockStatement = new TryBlockStatement(conn, parent, parser, id);
        parser.SkipToken(true);
        while (!parser.SkipSemicolons() && !parser.IsToken("END"))
          tryBlockStatement.Add(conn.ParseStatement((Statement) tryBlockStatement, id));
        parser.ExpectedExpression("END");
        parser.SkipToken(true);
        parser.ExpectedExpression("TRY");
        parser.SkipToken(true);
        parser.ExpectedExpression("BEGIN");
        parser.SkipToken(true);
        parser.ExpectedExpression("CATCH");
        parser.SkipToken(true);
        tryBlockStatement.SetFirstCatchStatement(tryBlockStatement.SubQueryCount);
        while (!parser.SkipSemicolons() && !parser.IsToken("END"))
          tryBlockStatement.Add(conn.ParseStatement((Statement) tryBlockStatement, id));
        parser.ExpectedExpression("END");
        parser.SkipToken(true);
        parser.ExpectedExpression("CATCH");
        parser.SkipToken(false);
        return (Statement) tryBlockStatement;
      }
    }
  }
}
