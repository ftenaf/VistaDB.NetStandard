using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class TemporaryResultSet : TempTable, IQuerySchemaInfo
  {
    private DataTable schema;

    internal TemporaryResultSet(IDatabase database)
      : base(database)
    {
    }

    public string GetAliasName(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public int GetColumnOrdinal(string name)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public int GetWidth(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsKey(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetColumnName(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetTableName(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public Type GetColumnType(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsAllowNull(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public VistaDBType GetColumnVistaDBType(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsAliased(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsExpression(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsAutoIncrement(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsLong(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsReadOnly(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetDataTypeName(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetColumnDescription(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetColumnCaption(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public bool GetIsEncrypted(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public int GetCodePage(int ordinal)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetIdentity(int ordinal, out string step, out string seed)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public string GetDefaultValue(int ordinal, out bool useInUpdate)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public DataTable GetSchemaTable()
    {
      if (schema != null)
        return schema;
      schema = BaseSelectStatement.GetSchemaTableInstance();
      schema.BeginLoadData();
      foreach (Row.Column column in (List<Row.Column>) patternRow)
      {
        DataRow row = schema.NewRow();
        row["ColumnName"] = column.Name;
        row["ColumnOrdinal"] = column.RowIndex;
        row["ColumnSize"] = column.MaxLength;
        row["NumericPrecision"] = (int)byte.MaxValue;
        row["NumericScale"] = (int)byte.MaxValue;
        row["IsUnique"] = false;
        row["IsKey"] = false;
        row["BaseColumnName"] = column.Name;
        row["BaseSchemaName"] = null;
        row["BaseTableName"] = "TempTable";
        row["DataType"] = column.SystemType;
        row["AllowDBNull"] = column.AllowNull;
        row["ProviderType"] = column.InternalType;
        row["IsAliased"] = false;
        row["IsExpression"] = false;
        row["IsIdentity"] = false;
        row["IsAutoIncrement"] = false;
        row["IsRowVersion"] = false;
        row["IsHidden"] = false;
        row["IsLong"] = false;
        row["IsReadOnly"] = true;
        row["ProviderSpecificDataType"] = column.SystemType;
        row["DataTypeName"] = column.InternalType.ToString();
        schema.Rows.Add(row);
      }
      schema.AcceptChanges();
      schema.EndLoadData();
      return schema;
    }

    public int ColumnCount
    {
      get
      {
        return patternRow.Count;
      }
    }
  }
}
