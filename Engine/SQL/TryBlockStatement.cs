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
      this.firstCatchStatementIndex = statementIndex;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int num = 0;
      foreach (Statement statement in (List<Statement>) this.statements)
      {
        if (num != this.firstCatchStatementIndex)
        {
          this.hasDDL = this.hasDDL || statement.HasDDLCommands;
          ++num;
        }
        else
          break;
      }
      return VistaDBType.Unknown;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (this.Exception != null)
      {
        this.currentStatement = this.firstCatchStatementIndex;
        this.Exception = (VistaDBException) null;
      }
      if (this.currentStatement >= this.statements.Count || this.breakBatch)
        return (INextQueryResult) null;
      this.connection.PrepareCLRContext(pipe);
      Statement statement;
      IQueryResult resultSet;
      try
      {
        statement = this.statements[this.currentStatement];
        try
        {
          resultSet = statement.ExecuteQuery();
        }
        catch (VistaDBException ex)
        {
          this.Connection.LastException = ex;
          if (this.currentStatement < this.firstCatchStatementIndex)
          {
            this.currentStatement = this.firstCatchStatementIndex;
            return this.NextResult(pipe);
          }
          this.parent.Exception = ex;
          return (INextQueryResult) null;
        }
        if (++this.currentStatement == this.firstCatchStatementIndex)
          this.currentStatement = this.statements.Count;
      }
      finally
      {
        this.connection.UnprepareCLRContext();
      }
      return (INextQueryResult) new BatchStatement.ResultSetData(resultSet, resultSet == null ? (IQuerySchemaInfo) null : statement.GetSchemaInfo(), statement.AffectedRows);
    }
  }
}
