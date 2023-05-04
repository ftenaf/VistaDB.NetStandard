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
        ParseFromClause(parser);
      ParseWhereClause(parser);
    }

    public override SourceTable GetTableByAlias(string tableAlias)
    {
      if (connection.CompareString(destinationTable.Alias, tableAlias, true) == 0)
        return destinationTable;
      return base.GetTableByAlias(tableAlias);
    }

    public override SearchColumnResult GetTableByColumnName(string columnName, out SourceTable table, out int columnIndex)
    {
      int columnOrdinal = destinationTable.Schema.GetColumnOrdinal(columnName);
      if (columnOrdinal < 0)
        return base.GetTableByColumnName(columnName, out table, out columnIndex);
      columnIndex = columnOrdinal;
      table = destinationTable;
      return SearchColumnResult.Found;
    }

    public override SourceTable GetSourceTable(int index)
    {
      return null;
    }

    protected override VistaDBType OnPrepareQuery()
    {
      int tableIndex = 0;
      IViewList views = Database.EnumViews();
      IVistaDBTableNameCollection tableNames = Database.GetTableNames();
      if (join != null)
      {
        join = join.PrepareTables(tableNames, views, sourceTables, false, ref tableIndex);
        if (join is NativeSourceTable && !sourceTables.Contains((SourceTable) join))
          sourceTables.AddTable(destinationTable);
      }
      int index1 = -1;
      if (sourceTables.Count > 0)
      {
        string tableName = destinationTable.TableName;
        for (int index2 = 0; index2 < sourceTables.Count; ++index2)
        {
          SourceTable sourceTable = sourceTables[index2];
          if (connection.CompareString(tableName, sourceTable.TableName, true) == 0 || connection.CompareString(tableName, sourceTable.Alias, true) == 0)
          {
            index1 = index2;
            if (sourceTable.Alias == "")
              break;
          }
        }
      }
      isTableInSourceList = index1 >= 0;
      if (index1 >= 0)
      {
        destinationTable = sourceTables[index1];
      }
      else
      {
        destinationTable = (SourceTable) destinationTable.PrepareTables(tableNames, views, null, false, ref tableIndex);
        destinationTable.Prepare();
        if (sourceTables.Count == 0)
        {
          sourceTables.AddTable(destinationTable);
          join = destinationTable;
        }
      }
      sourceTables.Prepare();
      PrepareOptimize();
      if (join != null)
        join.Prepare();
      PrepareSetColumns();
      whereClause.Prepare();
      destinationTable.Unprepare();
      destinationTable.ReadOnly = false;
      sourceTables.Unprepare();
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      affectedRows = 0L;
      Optimize();
      try
      {
        if (!destinationTable.Opened)
          destinationTable.Open();
        sourceTables.Open();
        destinationTable.First(constraintOperations);
        DoPrepareTriggers();
        bool justReset = true;
        try
        {
          if (!isTableInSourceList && sourceTables.Count == 1)
            ExecuteSimple();
          else
            ExecuteJoin();
          justReset = false;
        }
        finally
        {
          Connection.CachedAffectedRows = affectedRows;
          DoExecuteTriggers(justReset);
        }
      }
      catch
      {
        sourceTables.Close();
        destinationTable.Close();
        throw;
      }
      sourceTables.Free();
      destinationTable.FreeTable();
      SetChanged();
      return null;
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
      while (!destinationTable.Eof)
      {
        if (whereClause.Execute(false))
          AcceptRow();
        destinationTable.Next(constraintOperations);
      }
    }
  }
}
