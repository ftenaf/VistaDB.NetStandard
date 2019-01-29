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

    public BaseViewSourceTable(VistaDB.Engine.SQL.Statement parent, IView view, List<string> columnNames, SelectStatement statement, string alias, int index, int lineNo, int symbolNo)
      : base(parent, view.Name, alias, index, lineNo, symbolNo)
    {
      this.view = view;
      this.columnNames = columnNames;
      this.statement = statement;
      this.updateTable = (SourceTable) null;
    }

    private void CheckUpdatable()
    {
      if (this.updateTable == null || !this.updateTable.IsUpdatable)
        throw new VistaDBSQLException(605, this.tableName, this.lineNo, this.symbolNo);
    }

    public override void Post()
    {
      this.updateTable.Post();
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
      int num = (int) this.statement.PrepareQuery();
      return (IQuerySchemaInfo) this;
    }

    protected override void InternalInsert()
    {
      this.CheckUpdatable();
      this.updateTable.Insert();
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
      this.CheckUpdatable();
      this.updateTable.PutValue(columnIndex, columnValue);
    }

    protected override void InternalDeleteRow()
    {
      this.CheckUpdatable();
      this.updateTable.DeleteRow();
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
        if (this.updateTable != null)
          return this.updateTable.IsUpdatable;
        return false;
      }
    }

    public SelectStatement Statement
    {
      get
      {
        return this.statement;
      }
    }

    public string GetAliasName(int ordinal)
    {
      if (this.columnNames != null)
        return this.columnNames[ordinal];
      return this.statement.GetAliasName(ordinal);
    }

    public int GetColumnOrdinal(string name)
    {
      if (this.columnNames == null)
        return this.statement.GetColumnOrdinal(name);
      LocalSQLConnection connection = this.parent.Connection;
      int index = 0;
      for (int count = this.columnNames.Count; index < count; ++index)
      {
        if (connection.CompareString(this.columnNames[index], name, true) == 0)
          return index;
      }
      return -1;
    }

    public int GetWidth(int ordinal)
    {
      return this.statement.GetWidth(ordinal);
    }

    public bool GetIsKey(int ordinal)
    {
      return this.statement.GetIsKey(ordinal);
    }

    public string GetColumnName(int ordinal)
    {
      if (this.columnNames != null)
        return this.columnNames[ordinal];
      return this.statement.GetColumnName(ordinal);
    }

    public string GetTableName(int ordinal)
    {
      return this.statement.GetTableName(ordinal);
    }

    public Type GetColumnType(int ordinal)
    {
      return this.statement.GetColumnType(ordinal);
    }

    public bool GetIsAllowNull(int ordinal)
    {
      if (!this.alwaysAllowNull)
        return this.statement.GetIsAllowNull(ordinal);
      return true;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return this.statement.GetColumnVistaDBType(ordinal);
    }

    public bool GetIsAliased(int ordinal)
    {
      return this.statement.GetIsAliased(ordinal);
    }

    public bool GetIsExpression(int ordinal)
    {
      return this.statement.GetIsExpression(ordinal);
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      return this.statement.GetIsAutoIncrement(ordinal);
    }

    public bool GetIsLong(int ordinal)
    {
      return this.statement.GetIsLong(ordinal);
    }

    public bool GetIsReadOnly(int ordinal)
    {
      return this.statement.GetIsReadOnly(ordinal);
    }

    public string GetDataTypeName(int ordinal)
    {
      return this.statement.GetDataTypeName(ordinal);
    }

    public DataTable GetSchemaTable()
    {
      DataTable schemaTable = this.statement.GetSchemaTable();
      if (this.columnNames != null)
      {
        int index = 0;
        for (int count = schemaTable.Rows.Count; index < count; ++index)
        {
          schemaTable.Rows[index]["ColumnName"] = (object) this.columnNames[index];
          schemaTable.Rows[index]["BaseColumnName"] = (object) this.columnNames[index];
        }
      }
      return schemaTable;
    }

    public string GetColumnDescription(int ordinal)
    {
      return this.statement.GetColumnDescription(ordinal);
    }

    public string GetColumnCaption(int ordinal)
    {
      return this.statement.GetColumnCaption(ordinal);
    }

    public bool GetIsEncrypted(int ordinal)
    {
      return this.statement.GetIsEncrypted(ordinal);
    }

    public int GetCodePage(int ordinal)
    {
      return this.statement.GetCodePage(ordinal);
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
      return this.statement.GetIdentity(ordinal, out step, out seed);
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
      return this.statement.GetDefaultValue(ordinal, out useInUpdate);
    }

    public int ColumnCount
    {
      get
      {
        return this.statement.ColumnCount;
      }
    }
  }
}
