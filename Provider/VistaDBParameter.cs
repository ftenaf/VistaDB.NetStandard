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
      this.paramName = "";
      this.nativeParamName = "";
      this.direction = ParameterDirection.Input;
      this.isNullable = true;
      this.size = 0;
      this.sourceColumn = (string) null;
      this.sourceVersion = DataRowVersion.Default;
      this.paramValue = (object) DBNull.Value;
      this.paramType = VistaDBType.Unknown;
      this.dataTypeSet = false;
      this.prepared = false;
      this.sourceColumnNullMapping = false;
    }

    public VistaDBParameter(string parameterName, object value)
      : this()
    {
      this.ParameterName = parameterName;
      this.Value = value;
    }

    public VistaDBParameter(string parameterName, VistaDBType dataType)
      : this()
    {
      this.ParameterName = parameterName;
      this.VistaDBType = dataType;
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
      this.Value = value;
    }

    private VistaDBParameter(VistaDBParameter p)
      : this(p.ParameterName, p.VistaDBType, p.Size, p.Direction, p.IsNullable, p.SourceColumn, p.SourceVersion, p.Value)
    {
    }

    public override DbType DbType
    {
      get
      {
        return VistaDBParameter.ConvertVistaDBToDb(this.VistaDBType);
      }
      set
      {
        this.VistaDBType = VistaDBParameter.ConvertDbToVistaDB(value);
      }
    }

    public override ParameterDirection Direction
    {
      get
      {
        return this.direction;
      }
      set
      {
        this.direction = value;
      }
    }

    public override bool IsNullable
    {
      get
      {
        return this.isNullable;
      }
      set
      {
        this.isNullable = value;
      }
    }

    public override string ParameterName
    {
      get
      {
        return this.paramName;
      }
      set
      {
        this.paramName = value;
        if (this.paramName == null || this.paramName.Length == 0 || this.paramName[0] != '@')
          this.nativeParamName = this.paramName;
        else
          this.nativeParamName = this.paramName.Substring(1);
      }
    }

    public override int Size
    {
      get
      {
        return this.size;
      }
      set
      {
        this.size = value;
      }
    }

    public override string SourceColumn
    {
      get
      {
        return this.sourceColumn;
      }
      set
      {
        this.sourceColumn = value;
      }
    }

    public override bool SourceColumnNullMapping
    {
      get
      {
        return this.sourceColumnNullMapping;
      }
      set
      {
        this.sourceColumnNullMapping = value;
      }
    }

    public override DataRowVersion SourceVersion
    {
      get
      {
        return this.sourceVersion;
      }
      set
      {
        this.sourceVersion = value;
      }
    }

    public override object Value
    {
      get
      {
        return this.paramValue;
      }
      set
      {
        this.paramValue = value;
        this.prepared = false;
      }
    }

    public VistaDBType VistaDBType
    {
      get
      {
        return this.paramType;
      }
      set
      {
        this.paramType = value;
        this.dataTypeSet = true;
        this.prepared = false;
      }
    }

    public override void ResetDbType()
    {
      this.paramValue = (object) DBNull.Value;
      this.paramType = VistaDBType.Unknown;
      this.dataTypeSet = false;
      this.prepared = false;
    }

    private VistaDBType GetValueDataType()
    {
      if (this.paramValue == DBNull.Value || this.paramValue == null)
        return VistaDBType.Unknown;
      switch (Type.GetTypeCode(this.paramValue.GetType()))
      {
        case TypeCode.Empty:
        case TypeCode.DBNull:
          return VistaDBType.Unknown;
        case TypeCode.Object:
          if (this.paramValue.GetType() == typeof (Guid))
            return VistaDBType.UniqueIdentifier;
          if (this.paramValue.GetType() == typeof (byte[]))
            return VistaDBType.Image;
          throw new SystemException("Value is of unknown data type");
        case TypeCode.Boolean:
          return VistaDBType.Bit;
        case TypeCode.Char:
          this.paramValue = (object) ((char) this.paramValue).ToString();
          return VistaDBType.NChar;
        case TypeCode.SByte:
          this.paramValue = (object) (byte) (sbyte) this.paramValue;
          return VistaDBType.TinyInt;
        case TypeCode.Byte:
          return VistaDBType.TinyInt;
        case TypeCode.Int16:
          return VistaDBType.SmallInt;
        case TypeCode.UInt16:
          this.paramValue = (object) (short) (ushort) this.paramValue;
          return VistaDBType.SmallInt;
        case TypeCode.Int32:
          return VistaDBType.Int;
        case TypeCode.UInt32:
          this.paramValue = (object) (int) (uint) this.paramValue;
          return VistaDBType.Int;
        case TypeCode.Int64:
          return VistaDBType.BigInt;
        case TypeCode.UInt64:
          this.paramValue = (object) (long) (ulong) this.paramValue;
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
      if (index < 0 || index >= VistaDBParameter.VistaDBToDb.Length)
        throw new VistaDBException(1002, dataType.ToString());
      return VistaDBParameter.VistaDBToDb[index];
    }

    private static VistaDBType ConvertDbToVistaDB(DbType dataType)
    {
      int index = (int) dataType;
      if (index < 0 || index >= VistaDBParameter.DbToVistaDB.Length)
        throw new VistaDBException(1002, dataType.ToString());
      return VistaDBParameter.DbToVistaDB[index];
    }

    internal void Prepare()
    {
      if (this.prepared)
        return;
      VistaDBType valueDataType = this.GetValueDataType();
      this.prepared = true;
      if (!this.dataTypeSet)
      {
        if (valueDataType == VistaDBType.Unknown)
        {
          this.paramType = VistaDBType.NChar;
        }
        else
        {
          this.paramType = valueDataType;
          this.dataTypeSet = true;
        }
      }
      else
      {
        if (valueDataType == VistaDBType.Unknown || Row.Column.GetInternalType(valueDataType) == Row.Column.GetInternalType(this.paramType))
          return;
        VistaDBParameter.ParameterValue parameterValue1 = new VistaDBParameter.ParameterValue(valueDataType, this.paramValue);
        VistaDBParameter.ParameterValue parameterValue2 = new VistaDBParameter.ParameterValue(this.paramType, (object) null);
        VistaDBConnection.Conversion.Convert((IValue) parameterValue1, (IValue) parameterValue2);
        this.paramValue = parameterValue2.Value;
      }
    }

    internal string NativeParameterName
    {
      get
      {
        return this.nativeParamName;
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
        return this.paramType;
      }
      set
      {
        this.paramType = value;
      }
    }

    object IParameter.Value
    {
      get
      {
        if (this.paramValue == DBNull.Value)
          return (object) null;
        return this.paramValue;
      }
      set
      {
        this.paramValue = value == null ? (object) DBNull.Value : value;
        this.prepared = false;
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
          return this.value;
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
          return this.value;
        }
      }

      public VistaDBType InternalType
      {
        get
        {
          return Row.Column.GetInternalType(this.dataType);
        }
      }

      public bool IsNull
      {
        get
        {
          return this.value == null;
        }
      }

      public VistaDBType Type
      {
        get
        {
          return this.dataType;
        }
        set
        {
          this.dataType = value;
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
