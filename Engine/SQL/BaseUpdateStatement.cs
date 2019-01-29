using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal abstract class BaseUpdateStatement : BaseSelectStatement
  {
    protected SourceTable destinationTable;
    protected bool isTableInSourceList;

    public BaseUpdateStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      if (parser.IsToken("FROM"))
        this.ParseFromClause(parser);
      this.ParseWhereClause(parser);
    }

    public override SourceTable GetTableByAlias(string tableAlias)
    {
      if (this.connection.CompareString(this.destinationTable.Alias, tableAlias, true) == 0)
        return this.destinationTable;
      return base.GetTableByAlias(tableAlias);
    }

    public override SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      int columnOrdinal = this.destinationTable.Schema.GetColumnOrdinal(columnName);
      if (columnOrdinal < 0)
        return base.GetTableByColumnName(columnName, out table, out columnIndex);
      columnIndex = columnOrdinal;
      table = this.destinationTable;
      return SearchColumnResult.Found;
    }

    public override SourceTable GetSourceTable(int index)
    {
      return (SourceTable) null;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int tableIndex = 0;
      IViewList views = this.Database.EnumViews();
      IVistaDBTableNameCollection tableNames = this.Database.GetTableNames();
      if (this.join != null)
      {
        this.join = this.join.PrepareTables(tableNames, views, this.sourceTables, false, ref tableIndex);
        if (this.join is NativeSourceTable && !this.sourceTables.Contains((SourceTable) this.join))
          this.sourceTables.AddTable(this.destinationTable);
      }
      int index1 = -1;
      if (this.sourceTables.Count > 0)
      {
        string tableName = this.destinationTable.TableName;
        for (int index2 = 0; index2 < this.sourceTables.Count; ++index2)
        {
          SourceTable sourceTable = this.sourceTables[index2];
          if (this.connection.CompareString(tableName, sourceTable.TableName, true) == 0 || this.connection.CompareString(tableName, sourceTable.Alias, true) == 0)
          {
            index1 = index2;
            if (sourceTable.Alias == "")
              break;
          }
        }
      }
      this.isTableInSourceList = index1 >= 0;
      if (index1 >= 0)
      {
        this.destinationTable = this.sourceTables[index1];
      }
      else
      {
        this.destinationTable = (SourceTable) this.destinationTable.PrepareTables(tableNames, views, (TableCollection) null, false, ref tableIndex);
        this.destinationTable.Prepare();
        if (this.sourceTables.Count == 0)
        {
          this.sourceTables.AddTable(this.destinationTable);
          this.join = (IRowSet) this.destinationTable;
        }
      }
      this.sourceTables.Prepare();
      this.PrepareOptimize();
      if (this.join != null)
        this.join.Prepare();
      this.PrepareSetColumns();
      this.whereClause.Prepare();
      this.destinationTable.Unprepare();
      this.destinationTable.ReadOnly = false;
      this.sourceTables.Unprepare();
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      this.affectedRows = 0L;
      this.Optimize();
      try
      {
        if (!this.destinationTable.Opened)
          this.destinationTable.Open();
        this.sourceTables.Open();
        this.destinationTable.First(this.constraintOperations);
        this.DoPrepareTriggers();
        bool justReset = true;
        try
        {
          if (!this.isTableInSourceList && this.sourceTables.Count == 1)
            this.ExecuteSimple();
          else
            this.ExecuteJoin();
          justReset = false;
        }
        finally
        {
          this.Connection.CachedAffectedRows = this.affectedRows;
          this.DoExecuteTriggers(justReset);
        }
      }
      catch
      {
        this.sourceTables.Close();
        this.destinationTable.Close();
        throw;
      }
      this.sourceTables.Free();
      this.destinationTable.FreeTable();
      this.SetChanged();
      return (IQueryResult) null;
    }

    protected virtual void PrepareSetColumns()
    {
    }

    protected virtual void DoPrepareTriggers()
    {
    }

    protected virtual void DoExecuteTriggers(bool justReset)
    {
    }

    protected virtual void ExecuteSimple()
    {
      while (!this.destinationTable.Eof)
      {
        if (this.whereClause.Execute(false))
          this.AcceptRow();
        this.destinationTable.Next(this.constraintOperations);
      }
    }
  }
}
