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
      this.lineNo = parser.TokenValue.RowNo;
      this.symbolNo = parser.TokenValue.ColNo;
      this.id = id;
      this.DoBeforeParse();
      this.Parse(connection, parser);
      this.DoAfterParse(parser);
    }

    private void Parse(LocalSQLConnection connection, SQLParser parser)
    {
      string token = parser.TokenValue.Token;
      try
      {
        this.OnParse(connection, parser);
      }
      catch (System.Exception ex)
      {
        throw new VistaDBSQLException(ex, 509, token, this.lineNo, this.symbolNo);
      }
    }

    protected virtual void DoBeforeParse()
    {
    }

    protected abstract void OnParse(LocalSQLConnection connection, SQLParser parser);

    protected virtual void DoAfterParse(SQLParser parser)
    {
      this.name = parser.TokenValue.Token;
    }

    protected abstract VistaDBType OnPrepareQuery();

    protected abstract IQueryResult OnExecuteQuery();

    public virtual void DoSetParam(string paramName, object val, VistaDBType dataType, ParameterDirection direction)
    {
      this.parent.DoSetParam(paramName, val, dataType, direction);
    }

    public virtual void DoSetParam(string paramName, IParameter parameter)
    {
      this.parent.DoSetParam(paramName, parameter);
    }

    public virtual IParameter DoGetParam(string paramName)
    {
      return this.parent.DoGetParam(paramName);
    }

    public virtual IParameter DoGetReturnParameter()
    {
      return this.parent.DoGetReturnParameter();
    }

    public virtual CreateTableStatement DoGetTemporaryTableName(string paramName)
    {
      return this.parent.DoGetTemporaryTableName(paramName);
    }

    public virtual WhileStatement DoGetCycleStatement()
    {
      return this.parent.DoGetCycleStatement();
    }

    public virtual void DoRegisterTemporaryTableName(string paramName, CreateTableStatement createTableStatement)
    {
      this.parent.DoRegisterTemporaryTableName(paramName, createTableStatement);
    }

    public virtual void DoSetReturnParameter(IParameter param)
    {
      this.parent.DoSetReturnParameter(param);
    }

    public virtual void DoClearParams()
    {
      this.parent.DoClearParams();
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
        if (!this.connection.DatabaseOpened)
          throw new VistaDBSQLException(623, "", 0, 0);
        return this.connection.Database;
      }
    }

    public LocalSQLConnection Connection
    {
      get
      {
        return this.connection;
      }
    }

    internal VistaDBException Exception
    {
      get
      {
        return this.exception;
      }
      set
      {
        this.exception = value;
      }
    }

    internal virtual BatchStatement Batch
    {
      get
      {
        if (this.parent is BatchStatement)
          return this.parent as BatchStatement;
        if (this.parent != null)
          return this.parent.Batch;
        return (BatchStatement) null;
      }
    }

    public string CommandText
    {
      get
      {
        return this.commandText;
      }
      set
      {
        this.commandText = value;
      }
    }

    public string Name
    {
      get
      {
        return this.name;
      }
    }

    public bool HasDDLCommands
    {
      get
      {
        return this.hasDDL;
      }
    }

    public VistaDBType PrepareQuery()
    {
      if (!this.prepared)
      {
        this.prepared = true;
        return this.OnPrepareQuery();
      }
      if (this.alwaysPrepareList != null)
      {
        foreach (Signature alwaysPrepare in this.alwaysPrepareList)
        {
          int num = (int) alwaysPrepare.Prepare();
        }
      }
      return VistaDBType.Unknown;
    }

    public IQueryResult ExecuteQuery()
    {
      int num = (int) this.PrepareQuery();
      IQueryResult queryResult = this.OnExecuteQuery();
      if (this.parent is IFStatement && this.parent != null)
        this.parent.AffectedRows = this.AffectedRows;
      return queryResult;
    }

    public long Id
    {
      get
      {
        return this.id;
      }
    }

    public virtual long AffectedRows
    {
      get
      {
        return this.affectedRows;
      }
      set
      {
        this.affectedRows = value;
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
        return this.isDisposed;
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
        return this.disposingPostponed;
      }
      set
      {
        this.disposingPostponed = value;
        if (value || this.connection == null || !this.connection.QueryIsDisposed(this.Id))
          return;
        lock (this.connection.SyncRoot)
          this.connection.RemoveQuery(this.Id);
      }
    }

    public virtual void Dispose()
    {
      if (this.isDisposed)
        return;
      this.connection = (LocalSQLConnection) null;
      this.alwaysPrepareList = (List<Signature>) null;
      this.commandText = (string) null;
      this.isDisposed = true;
      GC.SuppressFinalize((object) this);
    }

    public void AlwaysPrepare(Signature signature)
    {
      if (this.parent != null)
      {
        this.parent.AlwaysPrepare(signature);
      }
      else
      {
        if (this.alwaysPrepareList == null)
          this.alwaysPrepareList = new List<Signature>();
        this.alwaysPrepareList.Add(signature);
      }
    }

    public void SetHasDDL()
    {
      this.hasDDL = true;
    }
  }
}
