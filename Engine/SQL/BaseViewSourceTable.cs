using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal abstract class BaseViewSourceTable : SourceTable, IQuerySchemaInfo
  {
    protected IView view;
    protected SelectStatement statement;
    protected SourceTable updateTable;
    private List<string> columnNames;

    public BaseViewSourceTable(Statement parent, IView view, List<string> columnNames, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, view.Name, alias, index, lineNo, symbolNo)
    {
      this.view = view;
      this.columnNames = columnNames;
      this.statement = statement;
      updateTable = (SourceTable) null;
    }

    private void CheckUpdatable()
    {
      if (updateTable == null || !updateTable.IsUpdatable)
        throw new VistaDBSQLException(605, tableName, lineNo, symbolNo);
    }

    public override void Post()
    {
      updateTable.Post();
    }

    public override IVistaDBTableSchema GetTableSchema()
    {
      return (IVistaDBTableSchema) null;
    }

    public override IColumn GetLastIdentity(string columnName)
    {
      return (IColumn) null;
    }

    public override string CreateIndex(string expression, bool instantly)
    {
      return (string) null;
    }

    protected override IQuerySchemaInfo InternalPrepare()
    {
      int num = (int) statement.PrepareQuery();
      return (IQuerySchemaInfo) this;
    }

    protected override void InternalInsert()
    {
      CheckUpdatable();
      updateTable.Insert();
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
      CheckUpdatable();
      updateTable.PutValue(columnIndex, columnValue);
    }

    protected override void InternalDeleteRow()
    {
      CheckUpdatable();
      updateTable.DeleteRow();
    }

    public override bool IsNativeTable
    {
      get
      {
        return false;
      }
    }

    public override bool IsUpdatable
    {
      get
      {
        if (updateTable != null)
          return updateTable.IsUpdatable;
        return false;
      }
    }

    public SelectStatement Statement
    {
      get
      {
        return statement;
      }
    }

    public string GetAliasName(int ordinal)
    {
      if (columnNames != null)
        return columnNames[ordinal];
      return statement.GetAliasName(ordinal);
    }

    public int GetColumnOrdinal(string name)
    {
      if (columnNames == null)
        return statement.GetColumnOrdinal(name);
      LocalSQLConnection connection = parent.Connection;
      int index = 0;
      for (int count = columnNames.Count; index < count; ++index)
      {
        if (connection.CompareString(columnNames[index], name, true) == 0)
          return index;
      }
      return -1;
    }

    public int GetWidth(int ordinal)
    {
      return statement.GetWidth(ordinal);
    }

    public bool GetIsKey(int ordinal)
    {
      return statement.GetIsKey(ordinal);
    }

    public string GetColumnName(int ordinal)
    {
      if (columnNames != null)
        return columnNames[ordinal];
      return statement.GetColumnName(ordinal);
    }

    public string GetTableName(int ordinal)
    {
      return statement.GetTableName(ordinal);
    }

    public Type GetColumnType(int ordinal)
    {
      return statement.GetColumnType(ordinal);
    }

    public bool GetIsAllowNull(int ordinal)
    {
      if (!alwaysAllowNull)
        return statement.GetIsAllowNull(ordinal);
      return true;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return statement.GetColumnVistaDBType(ordinal);
    }

    public bool GetIsAliased(int ordinal)
    {
      return statement.GetIsAliased(ordinal);
    }

    public bool GetIsExpression(int ordinal)
    {
      return statement.GetIsExpression(ordinal);
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      return statement.GetIsAutoIncrement(ordinal);
    }

    public bool GetIsLong(int ordinal)
    {
      return statement.GetIsLong(ordinal);
    }

    public bool GetIsReadOnly(int ordinal)
    {
      return statement.GetIsReadOnly(ordinal);
    }

    public string GetDataTypeName(int ordinal)
    {
      return statement.GetDataTypeName(ordinal);
    }

    public DataTable GetSchemaTable()
    {
      DataTable schemaTable = statement.GetSchemaTable();
      if (columnNames != null)
      {
        int index = 0;
        for (int count = schemaTable.Rows.Count; index < count; ++index)
        {
          schemaTable.Rows[index]["ColumnName"] = (object) columnNames[index];
          schemaTable.Rows[index]["BaseColumnName"] = (object) columnNames[index];
        }
      }
      return schemaTable;
    }

    public string GetColumnDescription(int ordinal)
    {
      return statement.GetColumnDescription(ordinal);
    }

    public string GetColumnCaption(int ordinal)
    {
      return statement.GetColumnCaption(ordinal);
    }

    public bool GetIsEncrypted(int ordinal)
    {
      return statement.GetIsEncrypted(ordinal);
    }

    public int GetCodePage(int ordinal)
    {
      return statement.GetCodePage(ordinal);
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
      return statement.GetIdentity(ordinal, out step, out seed);
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
      return statement.GetDefaultValue(ordinal, out useInUpdate);
    }

    public int ColumnCount
    {
      get
      {
        return statement.ColumnCount;
      }
    }
  }
}
