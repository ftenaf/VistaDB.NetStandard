using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal class BatchStatement : Statement
  {
    protected BatchStatement.StatementCollection statements = new BatchStatement.StatementCollection();
    protected Dictionary<string, IParameter> prms = new Dictionary<string, IParameter>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
    protected Dictionary<string, CreateTableStatement> tempTables = new Dictionary<string, CreateTableStatement>((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase);
    private bool cascadeReturnParam = true;
    private IParameter returnParameter;
    protected int currentStatement;
    protected bool breakBatch;
    protected bool breakScope;

    public BatchStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
    }

    protected override VistaDBType OnPrepareQuery()
    {
      foreach (Statement statement in (List<Statement>) this.statements)
      {
        int num = (int) statement.PrepareQuery();
        this.hasDDL = this.hasDDL || statement.HasDDLCommands;
      }
      return VistaDBType.Unknown;
    }

    public override void ResetResult()
    {
      this.currentStatement = 0;
      this.breakBatch = false;
      foreach (Statement statement in (List<Statement>) this.statements)
        statement.ResetResult();
    }

    public override IQueryStatement SubQuery(int index)
    {
      if (index < 0 || index >= this.statements.Count)
        throw new VistaDBSQLException(591, index.ToString(), 0, 0);
      return (IQueryStatement) this.statements[index];
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (this.statements.Count == 0)
        return (IQueryResult) null;
      VistaDBDataReader reader = new VistaDBDataReader((IQueryStatement) this, (VistaDBConnection) null, CommandBehavior.Default);
      if (reader.FieldCount > 0)
      {
        VistaDBContext.SQLChannel.Pipe.Send(reader);
      }
      else
      {
        this.affectedRows = (long) reader.RecordsAffected;
        reader.Dispose();
      }
      return (IQueryResult) null;
    }

    private INextQueryResult CheckBatchExceptions()
    {
      VistaDBException vistaDbException = (VistaDBException) null;
      foreach (Statement statement in (List<Statement>) this.statements)
      {
        VistaDBException exception = statement.Exception;
        if (exception != null)
          vistaDbException = vistaDbException != null ? new VistaDBException((Exception) vistaDbException, exception.ErrorId) : exception;
      }
      if (vistaDbException == null)
        return (INextQueryResult) null;
      throw vistaDbException;
    }

    public override INextQueryResult NextResult(VistaDBPipe pipe)
    {
      if (this.Exception != null)
        throw this.Exception;
      if (this.currentStatement >= this.statements.Count || this.breakBatch)
        return this.CheckBatchExceptions();
      Statement statement = this.statements[this.currentStatement];
      this.connection.PrepareCLRContext(pipe);
      IQueryResult resultSet = (IQueryResult) null;
      VistaDBException vistaDbException = (VistaDBException) null;
      try
      {
        resultSet = statement.ExecuteQuery();
      }
      catch (VistaDBException ex)
      {
        vistaDbException = ex;
        this.DropTemporaryTables();
        statement.Exception = ex;
      }
      finally
      {
        this.connection.UnprepareCLRContext();
        this.Connection.LastException = vistaDbException;
      }
      ++this.currentStatement;
      return (INextQueryResult) new BatchStatement.ResultSetData(resultSet, resultSet == null ? (IQuerySchemaInfo) null : statement.GetSchemaInfo(), statement.AffectedRows);
    }

    public override void DoSetParam(string paramName, IParameter parameter)
    {
      this.prms.Add(paramName, parameter);
    }

    public override void DoSetParam(string paramName, object val, VistaDBType dataType, ParameterDirection direction)
    {
      IParameter parameter = this.DoGetParam(paramName);
      if (parameter != null)
      {
        parameter.Value = val;
        parameter.DataType = dataType;
        parameter.Direction = direction;
      }
      else
        this.prms.Add(paramName, (IParameter) new BatchStatement.ParamInfo(val, dataType, direction));
    }

    public override IParameter DoGetParam(string paramName)
    {
      IParameter parameter1;
      if (this.parent == null)
      {
        parameter1 = (IParameter) null;
      }
      else
      {
        IParameter parameter2 = parameter1 = this.parent.DoGetParam(paramName);
      }
      IParameter parameter3 = parameter1;
      if (parameter3 == null)
      {
        if (!this.prms.ContainsKey(paramName))
          return (IParameter) null;
        parameter3 = this.prms[paramName];
      }
      return parameter3;
    }

    protected bool ReturnParamCascade
    {
      get
      {
        return this.cascadeReturnParam;
      }
      set
      {
        this.cascadeReturnParam = value;
      }
    }

    public override IParameter DoGetReturnParameter()
    {
      if (this.returnParameter == null)
      {
        foreach (IParameter parameter in this.prms.Values)
        {
          if (parameter.Direction == ParameterDirection.ReturnValue)
            return parameter;
        }
        if (this.cascadeReturnParam && this.parent is BatchStatement)
          return this.parent.DoGetReturnParameter();
      }
      return this.returnParameter;
    }

    public override void DoSetReturnParameter(IParameter param)
    {
      this.returnParameter = param;
    }

    public override void DoClearParams()
    {
      this.prms.Clear();
    }

    public override WhileStatement DoGetCycleStatement()
    {
      if (this.parent != null)
        return this.parent.DoGetCycleStatement();
      return (WhileStatement) null;
    }

    public override void DoRegisterTemporaryTableName(string paramName, CreateTableStatement createTableStatement)
    {
      createTableStatement.CreateUniqueName(paramName);
      this.tempTables.Add(paramName, createTableStatement);
    }

    public override CreateTableStatement DoGetTemporaryTableName(string paramName)
    {
      CreateTableStatement createTableStatement1;
      if (this.parent == null)
      {
        createTableStatement1 = (CreateTableStatement) null;
      }
      else
      {
        CreateTableStatement createTableStatement2 = createTableStatement1 = this.parent.DoGetTemporaryTableName(paramName);
      }
      CreateTableStatement createTableStatement3 = createTableStatement1;
      if (createTableStatement3 == null && this.tempTables.ContainsKey(paramName))
        createTableStatement3 = this.tempTables[paramName];
      return createTableStatement3;
    }

    public override int SubQueryCount
    {
      get
      {
        return this.statements.Count;
      }
    }

    public Statement this[int index]
    {
      get
      {
        return this.statements[index];
      }
    }

    public bool BreakFlag
    {
      set
      {
        this.breakBatch = value;
      }
    }

    public bool ScopeBreakFlag
    {
      set
      {
        this.BreakFlag = value;
        if (!value)
          return;
        for (BatchStatement batch = this.Batch; batch != null; batch = batch.Batch)
          batch.BreakFlag = value;
      }
    }

    public void Add(Statement statement)
    {
      this.hasDDL = this.hasDDL || statement.HasDDLCommands;
      this.statements.Add(statement);
    }

    public override void Dispose()
    {
      this.DropTemporaryTables();
      foreach (Statement statement in (List<Statement>) this.statements)
        statement.Dispose();
      this.prms.Clear();
      this.prms = (Dictionary<string, IParameter>) null;
      this.statements = (BatchStatement.StatementCollection) null;
      this.tempTables = (Dictionary<string, CreateTableStatement>) null;
      base.Dispose();
    }

    internal void DropTemporaryTables()
    {
      if (this.tempTables == null)
        return;
      try
      {
        foreach (CreateTableStatement createTableStatement in this.tempTables.Values)
        {
          try
          {
            this.Database.DropTable(createTableStatement.TableName);
            createTableStatement.Dispose();
          }
          catch (Exception ex)
          {
          }
        }
      }
      finally
      {
        this.tempTables.Clear();
      }
    }

    internal class ParamInfo : IParameter
    {
      private ParameterDirection direction = ParameterDirection.Input;
      private VistaDBType dataType = VistaDBType.Unknown;
      private object val;

      internal ParamInfo(object val, VistaDBType dataType, ParameterDirection direction)
      {
        this.val = val;
        this.dataType = dataType;
        this.direction = direction;
      }

      object IParameter.Value
      {
        get
        {
          return this.val;
        }
        set
        {
          this.val = value;
        }
      }

      VistaDBType IParameter.DataType
      {
        get
        {
          return this.dataType;
        }
        set
        {
          this.dataType = value;
        }
      }

      ParameterDirection IParameter.Direction
      {
        get
        {
          return this.direction;
        }
        set
        {
          this.direction = value;
        }
      }
    }

    internal class ResultSetData : INextQueryResult, IDisposable
    {
      private IQueryResult resultSet;
      private IQuerySchemaInfo schema;
      private long affectedRows;

      internal ResultSetData(IQueryResult resultSet, IQuerySchemaInfo schema, long affectedRows)
      {
        this.resultSet = resultSet;
        this.schema = schema;
        this.affectedRows = affectedRows;
      }

      IQueryResult INextQueryResult.ResultSet
      {
        get
        {
          return this.resultSet;
        }
      }

      IQuerySchemaInfo INextQueryResult.Schema
      {
        get
        {
          return this.schema;
        }
      }

      long INextQueryResult.AffectedRows
      {
        get
        {
          return this.affectedRows;
        }
      }

      public void Dispose()
      {
        if (this.resultSet != null)
          this.resultSet.Close();
        this.resultSet = (IQueryResult) null;
        this.schema = (IQuerySchemaInfo) null;
      }
    }

    internal class StatementCollection : List<Statement>
    {
      public StatementCollection()
      {
      }

      public StatementCollection(int initial)
        : base(initial)
      {
      }
    }
  }
}
