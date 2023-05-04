using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class TryBlockStatement : BatchStatement
  {
    private int firstCatchStatementIndex;

    internal TryBlockStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    internal void SetFirstCatchStatement(int statementIndex)
    {
      firstCatchStatementIndex = statementIndex;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = 0;
      foreach (Statement statement in (List<Statement>) statements)
      {
        if (num != firstCatchStatementIndex)
        {
          hasDDL = hasDDL || statement.HasDDLCommands;
          ++num;
        }
        else
          break;
      }
      return VistaDBType.Unknown;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (Exception != null)
      {
        currentStatement = firstCatchStatementIndex;
        Exception = (VistaDBException) null;
      }
      if (currentStatement >= statements.Count || breakBatch)
        return (INextQueryResult) null;
      connection.PrepareCLRContext(pipe);
      Statement statement;
      IQueryResult resultSet;
      try
      {
        statement = statements[currentStatement];
        try
        {
          resultSet = statement.ExecuteQuery();
        }
        catch (VistaDBException ex)
        {
          Connection.LastException = ex;
          if (currentStatement < firstCatchStatementIndex)
          {
            currentStatement = firstCatchStatementIndex;
            return NextResult(pipe);
          }
          parent.Exception = ex;
          return (INextQueryResult) null;
        }
        if (++currentStatement == firstCatchStatementIndex)
          currentStatement = statements.Count;
      }
      finally
      {
        connection.UnprepareCLRContext();
      }
      return (INextQueryResult) new ResultSetData(resultSet, resultSet == null ? (IQuerySchemaInfo) null : statement.GetSchemaInfo(), statement.AffectedRows);
    }
  }
}
