using System;
using System.Data;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal class MessageString : TempTable, IQuerySchemaInfo
  {
    private string message;
    private DataTable schema;

    internal MessageString(IDatabase database, string message)
      : base(database)
    {
      this.message = message;
      this.AddColumn("Message", VistaDBType.NVarChar);
      this.Insert();
      this.Post();
      this.FirstRow();
      this.curRow[0].Value = (object) message;
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
      if (this.schema != null)
        return this.schema;
      this.schema = BaseSelectStatement.GetSchemaTableInstance();
      this.schema.BeginLoadData();
      DataRow row = this.schema.NewRow();
      row["ColumnName"] = (object) "MESSAGE";
      row["ColumnOrdinal"] = (object) 0;
      row["ColumnSize"] = (object) this.message.Length;
      row["NumericPrecision"] = (object) (int) byte.MaxValue;
      row["NumericScale"] = (object) (int) byte.MaxValue;
      row["IsUnique"] = (object) false;
      row["IsKey"] = (object) false;
      row["BaseColumnName"] = (object) "MESSAGE";
      row["BaseSchemaName"] = (object) null;
      row["BaseTableName"] = (object) "MESSAGE";
      row["DataType"] = (object) typeof (string);
      row["AllowDBNull"] = (object) true;
      row["ProviderType"] = (object) 4;
      row["IsAliased"] = (object) false;
      row["IsExpression"] = (object) false;
      row["IsIdentity"] = (object) false;
      row["IsAutoIncrement"] = (object) false;
      row["IsRowVersion"] = (object) false;
      row["IsHidden"] = (object) false;
      row["IsLong"] = (object) false;
      row["IsReadOnly"] = (object) true;
      row["ProviderSpecificDataType"] = (object) typeof (string);
      row["DataTypeName"] = (object) VistaDBType.NVarChar.ToString();
      this.schema.Rows.Add(row);
      this.schema.AcceptChanges();
      this.schema.EndLoadData();
      return this.schema;
    }

    public int ColumnCount
    {
      get
      {
        return 1;
      }
    }
  }
}
