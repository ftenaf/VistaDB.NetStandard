using System;
using System.Data;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Core.Cryptography;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class FuncSourceTable : SourceTable, IQuerySchemaInfo
  {
    private ITableValuedFunction func;
    private bool opened;
    private Row row;
    private bool eof;
    private string[] resultColumnNames;

    public FuncSourceTable(Statement parent, ITableValuedFunction func, string alias, int index, int lineNo, int symbolNo)
      : base(parent, alias, alias, index, lineNo, symbolNo)
    {
      opened = false;
      this.func = func;
      row = (Row) null;
      eof = true;
      resultColumnNames = (string[]) null;
    }

    private void PrepareFirstOpen()
    {
      IDatabase database = parent.Database;
      VistaDBType[] resultColumnTypes = func.GetResultColumnTypes();
      resultColumnNames = func.GetResultColumnNames();
      row = Row.CreateInstance(0U, true, (Encryption) null, (int[]) null);
      int index = 0;
      for (int length = resultColumnTypes.Length; index < length; ++index)
        row.AppendColumn(database.CreateEmptyColumn(resultColumnTypes[index]));
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
      if (!opened)
        return;
      opened = false;
      func.Close();
    }

    public override void FreeTable()
    {
      if (!(func is TableValuedFunction))
        return;
      func.Close();
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
      func.Open();
      opened = true;
    }

    protected override bool OnFirst()
    {
      eof = !opened || !func.First((IRow) row);
      return !eof;
    }

    protected override bool OnNext()
    {
      eof = !func.GetNextResult((IRow) row);
      return !eof;
    }

    protected override IQuerySchemaInfo InternalPrepare()
    {
      int num = (int) func.Prepare();
      PrepareFirstOpen();
      return (IQuerySchemaInfo) this;
    }

    protected override void InternalInsert()
    {
      throw new VistaDBSQLException(609, "", lineNo, symbolNo);
    }

    protected override void InternalPutValue(int columnIndex, IColumn columnValue)
    {
      throw new VistaDBSQLException(609, "", lineNo, symbolNo);
    }

    protected override void InternalDeleteRow()
    {
      throw new VistaDBSQLException(609, "", lineNo, symbolNo);
    }

    public override bool Eof
    {
      get
      {
        return eof;
      }
    }

    public override bool IsNativeTable
    {
      get
      {
        return false;
      }
    }

    public override bool Opened
    {
      get
      {
        return opened;
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
      return resultColumnNames[ordinal];
    }

    public int GetColumnOrdinal(string name)
    {
      LocalSQLConnection connection = parent.Connection;
      int index = 0;
      for (int length = resultColumnNames.Length; index < length; ++index)
      {
        if (connection.CompareString(resultColumnNames[index], name, true) == 0)
          return index;
      }
      return -1;
    }

    public int GetWidth(int ordinal)
    {
      return 8192;
    }

    public bool GetIsKey(int ordinal)
    {
      return false;
    }

    public string GetColumnName(int ordinal)
    {
      return resultColumnNames[ordinal];
    }

    public string GetTableName(int ordinal)
    {
      return (string) null;
    }

    public Type GetColumnType(int ordinal)
    {
      return row[ordinal].SystemType;
    }

    public bool GetIsAllowNull(int ordinal)
    {
      return true;
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      return row[ordinal].Type;
    }

    public bool GetIsAliased(int ordinal)
    {
      return true;
    }

    public bool GetIsExpression(int ordinal)
    {
      return true;
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
      return false;
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
        return resultColumnNames.Length;
      }
    }

    public ITableValuedFunction Function
    {
      get
      {
        return func;
      }
    }
  }
}
