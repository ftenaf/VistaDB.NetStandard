using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;
using VistaDB.Provider;

namespace VistaDB.Engine.SQL
{
  internal abstract class Statement : IQueryStatement, IDisposable
  {
    private List<Signature> alwaysPrepareList;
    private string name;
    protected VistaDBException exception;
    protected bool prepared;
    protected long affectedRows;
    protected string commandText;
    protected bool singleRow;
    protected bool hasDDL;
    protected LocalSQLConnection connection;
    protected long id;
    protected int lineNo;
    protected int symbolNo;
    protected Statement parent;
    private bool disposingPostponed;
    private bool isDisposed;

    protected Statement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
    {
      this.connection = connection;
      this.parent = parent;
      parser.Parent = this;
      lineNo = parser.TokenValue.RowNo;
      symbolNo = parser.TokenValue.ColNo;
      this.id = id;
      DoBeforeParse();
      Parse(connection, parser);
      DoAfterParse(parser);
    }

    private void Parse(LocalSQLConnection connection, SQLParser parser)
    {
      string token = parser.TokenValue.Token;
      try
      {
        OnParse(connection, parser);
      }
      catch (Exception ex)
      {
        throw new VistaDBSQLException(ex, 509, token, lineNo, symbolNo);
      }
    }

    protected virtual void DoBeforeParse()
    {
    }

    protected abstract void OnParse(LocalSQLConnection connection, SQLParser parser);

    protected virtual void DoAfterParse(SQLParser parser)
    {
      name = parser.TokenValue.Token;
    }

    protected abstract VistaDBType OnPrepareQuery();

    protected abstract IQueryResult OnExecuteQuery();

    public virtual void DoSetParam(string paramName, object val, VistaDBType dataType, ParameterDirection direction)
    {
      parent.DoSetParam(paramName, val, dataType, direction);
    }

    public virtual void DoSetParam(string paramName, IParameter parameter)
    {
      parent.DoSetParam(paramName, parameter);
    }

    public virtual IParameter DoGetParam(string paramName)
    {
      return parent.DoGetParam(paramName);
    }

    public virtual IParameter DoGetReturnParameter()
    {
      return parent.DoGetReturnParameter();
    }

    public virtual CreateTableStatement DoGetTemporaryTableName(string paramName)
    {
      return parent.DoGetTemporaryTableName(paramName);
    }

    public virtual WhileStatement DoGetCycleStatement()
    {
      return parent.DoGetCycleStatement();
    }

    public virtual void DoRegisterTemporaryTableName(string paramName, CreateTableStatement createTableStatement)
    {
      parent.DoRegisterTemporaryTableName(paramName, createTableStatement);
    }

    public virtual void DoSetReturnParameter(IParameter param)
    {
      parent.DoSetReturnParameter(param);
    }

    public virtual void DoClearParams()
    {
      parent.DoClearParams();
    }

    public virtual SourceTable GetTableByAlias(string tableAlias)
    {
      return (SourceTable) null;
    }

    public virtual SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      table = (SourceTable) null;
      columnIndex = -1;
      return SearchColumnResult.NotFound;
    }

    public virtual SourceTable GetSourceTable(int index)
    {
      return (SourceTable) null;
    }

    public virtual IQueryStatement SubQuery(int index)
    {
      if (index != 0)
        throw new VistaDBSQLException(591, index.ToString(), 0, 0);
      return (IQueryStatement) this;
    }

    internal virtual ConstraintOperations ConstraintOperations
    {
      get
      {
        return (ConstraintOperations) null;
      }
    }

    public virtual int SubQueryCount
    {
      get
      {
        return 1;
      }
    }

    public virtual int SourceTableCount
    {
      get
      {
        return 0;
      }
    }

    public IDatabase Database
    {
      get
      {
        if (!connection.DatabaseOpened)
          throw new VistaDBSQLException(623, "", 0, 0);
        return connection.Database;
      }
    }

    public LocalSQLConnection Connection
    {
      get
      {
        return connection;
      }
    }

    internal VistaDBException Exception
    {
      get
      {
        return exception;
      }
      set
      {
        exception = value;
      }
    }

    internal virtual BatchStatement Batch
    {
      get
      {
        if (parent is BatchStatement)
          return parent as BatchStatement;
        if (parent != null)
          return parent.Batch;
        return (BatchStatement) null;
      }
    }

    public string CommandText
    {
      get
      {
        return commandText;
      }
      set
      {
        commandText = value;
      }
    }

    public string Name
    {
      get
      {
        return name;
      }
    }

    public bool HasDDLCommands
    {
      get
      {
        return hasDDL;
      }
    }

    public VistaDBType PrepareQuery()
    {
      if (!prepared)
      {
        prepared = true;
        return OnPrepareQuery();
      }
      if (alwaysPrepareList != null)
      {
        foreach (Signature alwaysPrepare in alwaysPrepareList)
        {
          int num = (int) alwaysPrepare.Prepare();
        }
      }
      return VistaDBType.Unknown;
    }

    public IQueryResult ExecuteQuery()
    {
      int num = (int) PrepareQuery();
      IQueryResult queryResult = OnExecuteQuery();
      if (parent is IFStatement && parent != null)
        parent.AffectedRows = AffectedRows;
      return queryResult;
    }

    public long Id
    {
      get
      {
        return id;
      }
    }

    public virtual long AffectedRows
    {
      get
      {
        return affectedRows;
      }
      set
      {
        affectedRows = value;
      }
    }

    public virtual IQuerySchemaInfo GetSchemaInfo()
    {
      return (IQuerySchemaInfo) null;
    }

    public bool Disposed
    {
      get
      {
        return isDisposed;
      }
    }

    public virtual void ResetResult()
    {
    }

    public virtual INextQueryResult NextResult(VistaDBPipe pipe)
    {
      return (INextQueryResult) null;
    }

    public bool LockedDisposing
    {
      get
      {
        return disposingPostponed;
      }
      set
      {
        disposingPostponed = value;
        if (value || connection == null || !connection.QueryIsDisposed(Id))
          return;
        lock (connection.SyncRoot)
          connection.RemoveQuery(Id);
      }
    }

    public virtual void Dispose()
    {
      if (isDisposed)
        return;
      connection = (LocalSQLConnection) null;
      alwaysPrepareList = (List<Signature>) null;
      commandText = (string) null;
      isDisposed = true;
      GC.SuppressFinalize((object) this);
    }

    public void AlwaysPrepare(Signature signature)
    {
      if (parent != null)
      {
        parent.AlwaysPrepare(signature);
      }
      else
      {
        if (alwaysPrepareList == null)
          alwaysPrepareList = new List<Signature>();
        alwaysPrepareList.Add(signature);
      }
    }

    public void SetHasDDL()
    {
      hasDDL = true;
    }
  }
}
