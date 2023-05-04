using System;
using System.Data;
using VistaDB.DDA;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class RowSourceTable : SourceTable, IQuerySchemaInfo
  {
    private Row row;

    public RowSourceTable(Statement parent, string tableName, Row row)
      : base(parent, tableName, tableName, 0, 0, 0)
    {
      this.row = row;
    }

        public void SetRow(Row row)
    {
      ++dataVersion;
      this.row = row;
    }

    public override IColumn SimpleGetColumn(int colIndex)
    {
      return (IColumn) row[colIndex];
    }

    public override void Post()
    {
    }

    public override void Close()
    {
    }

    public override void FreeTable()
    {
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

    public override int GetColumnCount()
    {
      return ColumnCount;
    }

    protected override void OnOpen(bool readOnly)
    {
    }

    protected override bool OnFirst()
    {
      return true;
    }

    protected override bool OnNext()
    {
      return true;
    }

    protected override IQuerySchemaInfo InternalPrepare()
    {
      return (IQuerySchemaInfo) this;
    }

    protected override void InternalInsert()
    {
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
    }

    protected override void InternalDeleteRow()
    {
    }

    public override bool Eof
    {
      get
      {
        return false;
      }
    }

    public override bool IsNativeTable
    {
      get
      {
        return true;
      }
    }

    public override bool Opened
    {
      get
      {
        return true;
      }
    }

    public override bool IsUpdatable
    {
      get
      {
        return false;
      }
    }

    public string GetAliasName(int ordinal)
    {
      return row[ordinal].Name;
    }

    public int GetColumnOrdinal(string name)
    {
      Row.Column column = (Row.Column) ((IVistaDBRow) row)[name];
      if (!(column == (Row.Column) null))
        return column.RowIndex;
      return -1;
    }

    public int GetWidth(int ordinal)
    {
      return row[ordinal].MaxLength;
    }

    public bool GetIsKey(int ordinal)
    {
      return false;
    }

    public string GetColumnName(int ordinal)
    {
      return row[ordinal].Name;
    }

    public string GetTableName(int ordinal)
    {
      return tableName;
    }

    public Type GetColumnType(int ordinal)
    {
      return row[ordinal].SystemType;
    }

    public bool GetIsAllowNull(int ordinal)
    {
      if (!alwaysAllowNull)
        return row[ordinal].AllowNull;
      return true;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return row[ordinal].Type;
    }

    public bool GetIsAliased(int ordinal)
    {
      return false;
    }

    public bool GetIsExpression(int ordinal)
    {
      return false;
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      return false;
    }

    public bool GetIsLong(int ordinal)
    {
      return false;
    }

    public bool GetIsReadOnly(int ordinal)
    {
      return row[ordinal].ReadOnly;
    }

    public string GetDataTypeName(int ordinal)
    {
      return row[ordinal].Type.ToString();
    }

    public DataTable GetSchemaTable()
    {
      return (DataTable) null;
    }

    public string GetColumnDescription(int ordinal)
    {
      return (string) null;
    }

    public string GetColumnCaption(int ordinal)
    {
      return (string) null;
    }

    public bool GetIsEncrypted(int ordinal)
    {
      return false;
    }

    public int GetCodePage(int ordinal)
    {
      return 0;
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
      step = (string) null;
      seed = (string) null;
      return (string) null;
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
      useInUpdate = false;
      return (string) null;
    }

    public int ColumnCount
    {
      get
      {
        return row.Count;
      }
    }
  }
}
