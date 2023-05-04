using System;
using System.Data;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Compatibility.SqlServer
{
  public class SqlDataRecord : IDataRecord
  {
        private TemporaryResultSet _data;

    public SqlDataRecord(params SqlMetaData[] metaData)
    {
      _data = !SqlContext.IsAvailable ? new TemporaryResultSet((IDatabase) null) : new TemporaryResultSet(((LocalSQLConnection) VistaDBContext.SQLChannel.CurrentConnection).Database);
      int index = 0;
      for (int length = metaData.Length; index < length; ++index)
        _data.AddColumn(metaData[index].Name, metaData[index].VistaDBType, metaData[index].AllowNull, metaData[index].MaxLength);
      _data.FinalizeCreate();
      _data.Insert();
    }

    internal TemporaryResultSet DataTable
    {
      get
      {
        return _data;
      }
    }

    public int FieldCount
    {
      get
      {
        return _data.ColumnCount;
      }
    }

    public bool GetBoolean(int i)
    {
      return (bool) _data.GetValue(i, VistaDBType.Bit);
    }

    public byte GetByte(int i)
    {
      return (byte) _data.GetValue(i, VistaDBType.TinyInt);
    }

    public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
    {
      Buffer.BlockCopy((Array) _data.GetValue(i, VistaDBType.Image), (int) fieldOffset, (Array) buffer, bufferoffset, length);
      return (long) length;
    }

    public char GetChar(int i)
    {
      throw new NotImplementedException();
    }

    public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
    {
      ((string) _data.GetValue(i, VistaDBType.NChar)).CopyTo((int) fieldoffset, buffer, bufferoffset, length);
      return (long) length;
    }

    public IDataReader GetData(int i)
    {
      return (IDataReader) null;
    }

    public string GetDataTypeName(int i)
    {
      return _data.GetDataTypeName(i);
    }

    public DateTime GetDateTime(int i)
    {
      return (DateTime) _data.GetValue(i, VistaDBType.DateTime);
    }

    public Decimal GetDecimal(int i)
    {
      return (Decimal) _data.GetValue(i, VistaDBType.Decimal);
    }

    public double GetDouble(int i)
    {
      return (double) _data.GetValue(i, VistaDBType.Float);
    }

    public Type GetFieldType(int i)
    {
      return _data.GetColumnType(i);
    }

    public float GetFloat(int i)
    {
      return (float) _data.GetValue(i, VistaDBType.Real);
    }

    public Guid GetGuid(int i)
    {
      return (Guid) _data.GetValue(i, VistaDBType.UniqueIdentifier);
    }

    public short GetInt16(int i)
    {
      return (short) _data.GetValue(i, VistaDBType.SmallInt);
    }

    public int GetInt32(int i)
    {
      return (int) _data.GetValue(i, VistaDBType.Int);
    }

    public long GetInt64(int i)
    {
      return (long) _data.GetValue(i, VistaDBType.BigInt);
    }

    public string GetName(int i)
    {
      return _data.GetColumnName(i);
    }

    public int GetOrdinal(string name)
    {
      return _data.GetColumnOrdinal(name);
    }

    public string GetString(int i)
    {
      return (string) _data.GetValue(i, VistaDBType.NVarChar);
    }

    public object GetValue(int i)
    {
      return _data.GetValue(i, VistaDBType.Unknown);
    }

    public int GetValues(object[] values)
    {
      int num = values.Length < FieldCount ? values.Length : FieldCount;
      for (int index = 0; index < num; ++index)
        values[index] = _data.GetValue(index, VistaDBType.Unknown);
      return num;
    }

    public bool IsDBNull(int i)
    {
      return _data.IsNull(i);
    }

    public object this[string name]
    {
      get
      {
        return this[GetOrdinal(name)];
      }
    }

    public object this[int i]
    {
      get
      {
        return GetValue(i);
      }
    }

    public void SetBoolean(int i, bool value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetByte(int i, byte value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
    {
      byte[] numArray1 = (byte[]) _data.CurrentRow[i].Value;
      if (numArray1 == null || (long) numArray1.Length < fieldOffset + (long) length)
      {
        byte[] numArray2 = new byte[fieldOffset + (long) length];
        if (numArray1 != null && fieldOffset > 0L)
          Buffer.BlockCopy((Array) numArray1, 0, (Array) numArray2, 0, (int) fieldOffset);
        numArray1 = numArray2;
      }
      Buffer.BlockCopy((Array) buffer, bufferoffset, (Array) numArray1, (int) fieldOffset, length);
      _data.CurrentRow[i].Value = (object) numArray1;
    }

    public void SetChar(int i, char value)
    {
      throw new NotImplementedException();
    }

    public void SetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
    {
      throw new NotImplementedException();
    }

    public void SetDateTime(int i, DateTime value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetDecimal(int i, Decimal value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetDouble(int i, double value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetFloat(int i, float value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetGuid(int i, Guid value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetInt16(int i, short value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetInt32(int i, int value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetInt64(int i, long value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetString(int i, string value)
    {
      _data.CurrentRow[i].Value = (object) value;
    }

    public void SetValue(int i, object value)
    {
      _data.CurrentRow[i].Value = value;
    }

    public void SetValues(params object[] values)
    {
      int index = 0;
      for (int length = values.Length; index < length; ++index)
        _data.CurrentRow[index].Value = values[index];
    }
  }
}
