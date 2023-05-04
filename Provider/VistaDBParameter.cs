using System;
using System.Data;
using System.Data.Common;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Provider
{
  public class VistaDBParameter : DbParameter, ICloneable, IParameter
  {
    private static DbType[] VistaDBToDb = new DbType[25]
    {
      DbType.Object,
      DbType.AnsiStringFixedLength,
      DbType.StringFixedLength,
      DbType.AnsiString,
      DbType.String,
      DbType.AnsiString,
      DbType.String,
      DbType.Object,
      DbType.Byte,
      DbType.Int16,
      DbType.Int32,
      DbType.Int64,
      DbType.Single,
      DbType.Double,
      DbType.Decimal,
      DbType.Currency,
      DbType.Currency,
      DbType.Boolean,
      DbType.Object,
      DbType.DateTime,
      DbType.Object,
      DbType.Binary,
      DbType.Guid,
      DbType.DateTime,
      DbType.Int64
    };
    private static VistaDBType[] DbToVistaDB = new VistaDBType[26]
    {
      VistaDBType.VarChar,
      VistaDBType.VarBinary,
      VistaDBType.TinyInt,
      VistaDBType.Bit,
      VistaDBType.Money,
      VistaDBType.DateTime,
      VistaDBType.DateTime,
      VistaDBType.Decimal,
      VistaDBType.Float,
      VistaDBType.UniqueIdentifier,
      VistaDBType.SmallInt,
      VistaDBType.Int,
      VistaDBType.BigInt,
      VistaDBType.UniqueIdentifier,
      VistaDBType.TinyInt,
      VistaDBType.Real,
      VistaDBType.NVarChar,
      VistaDBType.DateTime,
      VistaDBType.SmallInt,
      VistaDBType.Int,
      VistaDBType.BigInt,
      VistaDBType.Float,
      VistaDBType.Char,
      VistaDBType.NChar,
      VistaDBType.Unknown,
      VistaDBType.NText
    };
    private string paramName;
    private string nativeParamName;
    private ParameterDirection direction;
    private bool isNullable;
    private int size;
    private string sourceColumn;
    private DataRowVersion sourceVersion;
    private object paramValue;
    private VistaDBType paramType;
    private bool dataTypeSet;
    private bool prepared;
    private bool sourceColumnNullMapping;

    public VistaDBParameter()
    {
      paramName = "";
      nativeParamName = "";
      direction = ParameterDirection.Input;
      isNullable = true;
      size = 0;
      sourceColumn = (string) null;
      sourceVersion = DataRowVersion.Default;
      paramValue = (object) DBNull.Value;
      paramType = VistaDBType.Unknown;
      dataTypeSet = false;
      prepared = false;
      sourceColumnNullMapping = false;
    }

    public VistaDBParameter(string parameterName, object value)
      : this()
    {
      ParameterName = parameterName;
      Value = value;
    }

    public VistaDBParameter(string parameterName, VistaDBType dataType)
      : this()
    {
      ParameterName = parameterName;
      VistaDBType = dataType;
    }

    public VistaDBParameter(string parameterName, VistaDBType dataType, int size)
      : this(parameterName, dataType)
    {
      this.size = size;
    }

    public VistaDBParameter(string parameterName, VistaDBType dataType, int size, string sourceColumn)
      : this(parameterName, dataType, size)
    {
      this.sourceColumn = sourceColumn;
    }

    public VistaDBParameter(string parameterName, VistaDBType dataType, int size, ParameterDirection direction, bool isNullable, string sourceColumn, DataRowVersion sourceVersion, object value)
      : this(parameterName, dataType, size, sourceColumn)
    {
      this.direction = direction;
      this.isNullable = isNullable;
      this.sourceVersion = sourceVersion;
      Value = value;
    }

    private VistaDBParameter(VistaDBParameter p)
      : this(p.ParameterName, p.VistaDBType, p.Size, p.Direction, p.IsNullable, p.SourceColumn, p.SourceVersion, p.Value)
    {
    }

    public override DbType DbType
    {
      get
      {
        return ConvertVistaDBToDb(VistaDBType);
      }
      set
      {
        VistaDBType = ConvertDbToVistaDB(value);
      }
    }

    public override ParameterDirection Direction
    {
      get
      {
        return direction;
      }
      set
      {
        direction = value;
      }
    }

    public override bool IsNullable
    {
      get
      {
        return isNullable;
      }
      set
      {
        isNullable = value;
      }
    }

    public override string ParameterName
    {
      get
      {
        return paramName;
      }
      set
      {
        paramName = value;
        if (paramName == null || paramName.Length == 0 || paramName[0] != '@')
          nativeParamName = paramName;
        else
          nativeParamName = paramName.Substring(1);
      }
    }

    public override int Size
    {
      get
      {
        return size;
      }
      set
      {
        size = value;
      }
    }

    public override string SourceColumn
    {
      get
      {
        return sourceColumn;
      }
      set
      {
        sourceColumn = value;
      }
    }

    public override bool SourceColumnNullMapping
    {
      get
      {
        return sourceColumnNullMapping;
      }
      set
      {
        sourceColumnNullMapping = value;
      }
    }

    public override DataRowVersion SourceVersion
    {
      get
      {
        return sourceVersion;
      }
      set
      {
        sourceVersion = value;
      }
    }

    public override object Value
    {
      get
      {
        return paramValue;
      }
      set
      {
        paramValue = value;
        prepared = false;
      }
    }

    public VistaDBType VistaDBType
    {
      get
      {
        return paramType;
      }
      set
      {
        paramType = value;
        dataTypeSet = true;
        prepared = false;
      }
    }

    public override void ResetDbType()
    {
      paramValue = (object) DBNull.Value;
      paramType = VistaDBType.Unknown;
      dataTypeSet = false;
      prepared = false;
    }

    private VistaDBType GetValueDataType()
    {
      if (paramValue == DBNull.Value || paramValue == null)
        return VistaDBType.Unknown;
      switch (Type.GetTypeCode(paramValue.GetType()))
      {
        case TypeCode.Empty:
        case TypeCode.DBNull:
          return VistaDBType.Unknown;
        case TypeCode.Object:
          if (paramValue.GetType() == typeof (Guid))
            return VistaDBType.UniqueIdentifier;
          if (paramValue.GetType() == typeof (byte[]))
            return VistaDBType.Image;
          throw new SystemException("Value is of unknown data type");
        case TypeCode.Boolean:
          return VistaDBType.Bit;
        case TypeCode.Char:
          paramValue = (object) ((char) paramValue).ToString();
          return VistaDBType.NChar;
        case TypeCode.SByte:
          paramValue = (object) (byte) (sbyte) paramValue;
          return VistaDBType.TinyInt;
        case TypeCode.Byte:
          return VistaDBType.TinyInt;
        case TypeCode.Int16:
          return VistaDBType.SmallInt;
        case TypeCode.UInt16:
          paramValue = (object) (short) (ushort) paramValue;
          return VistaDBType.SmallInt;
        case TypeCode.Int32:
          return VistaDBType.Int;
        case TypeCode.UInt32:
          paramValue = (object) (int) (uint) paramValue;
          return VistaDBType.Int;
        case TypeCode.Int64:
          return VistaDBType.BigInt;
        case TypeCode.UInt64:
          paramValue = (object) (long) (ulong) paramValue;
          return VistaDBType.BigInt;
        case TypeCode.Single:
          return VistaDBType.Real;
        case TypeCode.Double:
          return VistaDBType.Float;
        case TypeCode.Decimal:
          return VistaDBType.Decimal;
        case TypeCode.DateTime:
          return VistaDBType.DateTime;
        case TypeCode.String:
          return VistaDBType.NChar;
        default:
          throw new SystemException("Value is of unknown data type");
      }
    }

    private static DbType ConvertVistaDBToDb(VistaDBType dataType)
    {
      if (dataType == VistaDBType.Unknown)
        return DbType.Object;
      int index = (int) dataType;
      if (index < 0 || index >= VistaDBToDb.Length)
        throw new VistaDBException(1002, dataType.ToString());
      return VistaDBToDb[index];
    }

    private static VistaDBType ConvertDbToVistaDB(DbType dataType)
    {
      int index = (int) dataType;
      if (index < 0 || index >= DbToVistaDB.Length)
        throw new VistaDBException(1002, dataType.ToString());
      return DbToVistaDB[index];
    }

    internal void Prepare()
    {
      if (prepared)
        return;
      VistaDBType valueDataType = GetValueDataType();
      prepared = true;
      if (!dataTypeSet)
      {
        if (valueDataType == VistaDBType.Unknown)
        {
          paramType = VistaDBType.NChar;
        }
        else
        {
          paramType = valueDataType;
          dataTypeSet = true;
        }
      }
      else
      {
        if (valueDataType == VistaDBType.Unknown || Row.Column.GetInternalType(valueDataType) == Row.Column.GetInternalType(paramType))
          return;
                ParameterValue parameterValue1 = new ParameterValue(valueDataType, paramValue);
                ParameterValue parameterValue2 = new ParameterValue(paramType, (object) null);
        VistaDBConnection.Conversion.Convert((IValue) parameterValue1, (IValue) parameterValue2);
        paramValue = parameterValue2.Value;
      }
    }

    internal string NativeParameterName
    {
      get
      {
        return nativeParamName;
      }
    }

    object ICloneable.Clone()
    {
      return (object) new VistaDBParameter(this);
    }

    VistaDBType IParameter.DataType
    {
      get
      {
        return paramType;
      }
      set
      {
        paramType = value;
      }
    }

    object IParameter.Value
    {
      get
      {
        if (paramValue == DBNull.Value)
          return (object) null;
        return paramValue;
      }
      set
      {
        paramValue = value == null ? (object) DBNull.Value : value;
        prepared = false;
      }
    }

    private class ParameterValue : IValue, IVistaDBValue
    {
      private object value;
      private VistaDBType dataType;

      public ParameterValue(VistaDBType dataType, object value)
      {
        this.dataType = dataType;
        this.value = value;
      }

      public object Value
      {
        get
        {
          return value;
        }
        set
        {
          this.value = value;
        }
      }

      public object TrimmedValue
      {
        get
        {
          return value;
        }
      }

      public VistaDBType InternalType
      {
        get
        {
          return Row.Column.GetInternalType(dataType);
        }
      }

      public bool IsNull
      {
        get
        {
          return value == null;
        }
      }

      public VistaDBType Type
      {
        get
        {
          return dataType;
        }
        set
        {
          dataType = value;
        }
      }

      public Type SystemType
      {
        get
        {
          return (Type) null;
        }
      }
    }
  }
}
