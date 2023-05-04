using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Reflection;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.VistaDBTypes;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CLRStoredProcedure : Function
  {
    private static IDictionary<Type, VistaDBType> TypesMap = InitializeTypeMap();
    private static IDictionary<VistaDBType, Type> TypesReMap = InitializeTypeReMap();
    protected string procedureName;
    protected MethodInfo method;
    protected MethodInfo fillRow;
    private VistaDBValue[] values;
    private List<OutParameter> outParams;

    public CLRStoredProcedure(SQLParser parser, string procedureName)
      : base(parser, -1, false)
    {
      this.procedureName = procedureName;
      method = (MethodInfo) null;
      fillRow = (MethodInfo) null;
      values = (VistaDBValue[]) null;
      outParams = (List<OutParameter>) null;
      skipNull = false;
    }

    private static IDictionary<Type, VistaDBType> InitializeTypeMap()
    {
      IDictionary<Type, VistaDBType> dictionary = (IDictionary<Type, VistaDBType>) new Dictionary<Type, VistaDBType>();
      dictionary.Add(typeof (VistaDBString), VistaDBType.NChar);
      dictionary.Add(typeof (string), VistaDBType.NVarChar);
      dictionary.Add(typeof (SqlString), VistaDBType.NVarChar);
      dictionary.Add(typeof (VistaDBDateTime), VistaDBType.DateTime);
      dictionary.Add(typeof (DateTime), VistaDBType.DateTime);
      dictionary.Add(typeof (SqlDateTime), VistaDBType.DateTime);
      dictionary.Add(typeof (VistaDBBoolean), VistaDBType.Bit);
      dictionary.Add(typeof (bool), VistaDBType.Bit);
      dictionary.Add(typeof (SqlBoolean), VistaDBType.Bit);
      dictionary.Add(typeof (VistaDBByte), VistaDBType.TinyInt);
      dictionary.Add(typeof (byte), VistaDBType.TinyInt);
      dictionary.Add(typeof (SqlByte), VistaDBType.TinyInt);
      dictionary.Add(typeof (VistaDBInt16), VistaDBType.SmallInt);
      dictionary.Add(typeof (short), VistaDBType.SmallInt);
      dictionary.Add(typeof (SqlInt16), VistaDBType.SmallInt);
      dictionary.Add(typeof (VistaDBInt32), VistaDBType.Int);
      dictionary.Add(typeof (int), VistaDBType.Int);
      dictionary.Add(typeof (SqlInt32), VistaDBType.Int);
      dictionary.Add(typeof (VistaDBInt64), VistaDBType.BigInt);
      dictionary.Add(typeof (long), VistaDBType.BigInt);
      dictionary.Add(typeof (SqlInt64), VistaDBType.BigInt);
      dictionary.Add(typeof (VistaDBDecimal), VistaDBType.Decimal);
      dictionary.Add(typeof (Decimal), VistaDBType.Decimal);
      dictionary.Add(typeof (SqlDecimal), VistaDBType.Decimal);
      dictionary.Add(typeof (VistaDBBinary), VistaDBType.VarBinary);
      dictionary.Add(typeof (byte[]), VistaDBType.VarBinary);
      dictionary.Add(typeof (SqlBytes), VistaDBType.VarBinary);
      dictionary.Add(typeof (VistaDBGuid), VistaDBType.UniqueIdentifier);
      dictionary.Add(typeof (Guid), VistaDBType.UniqueIdentifier);
      dictionary.Add(typeof (SqlGuid), VistaDBType.UniqueIdentifier);
      dictionary.Add(typeof (VistaDBDouble), VistaDBType.Float);
      dictionary.Add(typeof (double), VistaDBType.Float);
      dictionary.Add(typeof (SqlDouble), VistaDBType.Float);
      dictionary.Add(typeof (VistaDBSingle), VistaDBType.Real);
      dictionary.Add(typeof (float), VistaDBType.Real);
      dictionary.Add(typeof (SqlSingle), VistaDBType.Real);
      dictionary.Add(typeof (void), VistaDBType.Unknown);
      return dictionary;
    }

    private static IDictionary<VistaDBType, Type> InitializeTypeReMap()
    {
      IDictionary<VistaDBType, Type> dictionary = (IDictionary<VistaDBType, Type>) new Dictionary<VistaDBType, Type>();
      dictionary.Add(VistaDBType.NChar, typeof (VistaDBString));
      dictionary.Add(VistaDBType.NVarChar, typeof (VistaDBString));
      dictionary.Add(VistaDBType.DateTime, typeof (VistaDBDateTime));
      dictionary.Add(VistaDBType.Bit, typeof (VistaDBBoolean));
      dictionary.Add(VistaDBType.TinyInt, typeof (VistaDBByte));
      dictionary.Add(VistaDBType.SmallInt, typeof (VistaDBInt16));
      dictionary.Add(VistaDBType.Int, typeof (VistaDBInt32));
      dictionary.Add(VistaDBType.BigInt, typeof (VistaDBInt64));
      dictionary.Add(VistaDBType.VarBinary, typeof (VistaDBBinary));
      dictionary.Add(VistaDBType.UniqueIdentifier, typeof (VistaDBGuid));
      dictionary.Add(VistaDBType.Float, typeof (VistaDBDouble));
      dictionary.Add(VistaDBType.Real, typeof (VistaDBSingle));
      return dictionary;
    }

    private static Type GetParameterType(ParameterInfo param)
    {
      if (!param.IsOut)
        return param.ParameterType;
      Type parameterType = param.ParameterType;
      string fullName = parameterType.FullName;
      string str = parameterType.AssemblyQualifiedName.Substring(fullName.Length);
      return Type.GetType(fullName.Substring(0, fullName.Length - 1) + str);
    }

    private VistaDBType ConvertType(Type type)
    {
      VistaDBType vistaDbType;
      if (!TypesMap.TryGetValue(type, out vistaDbType))
        throw new VistaDBSQLException(614, procedureName, lineNo, symbolNo);
      return vistaDbType;
    }

    private Type ConvertType(VistaDBType type)
    {
      Type type1;
      if (!TypesReMap.TryGetValue(type, out type1))
        throw new VistaDBSQLException(614, procedureName, lineNo, symbolNo);
      return type1;
    }

    private void PrepareProcedure()
    {
      try
      {
        method = parent.Database.PrepareInvoke(procedureName, out fillRow);
      }
      catch (Exception ex)
      {
        throw new VistaDBSQLException(ex, 607, procedureName, lineNo, symbolNo);
      }
      if (fillRow == null)
        dataType = ConvertType(method.ReturnType);
      else
        dataType = VistaDBType.Unknown;
      ParameterInfo[] parameters = method.GetParameters();
      if (parameters.Length != ParamCount)
        throw new VistaDBSQLException(501, procedureName, lineNo, symbolNo);
      values = new VistaDBValue[ParamCount];
      int valueIndex = 0;
      for (int paramCount = ParamCount; valueIndex < paramCount; ++valueIndex)
      {
        VistaDBType vistaDbType = GetVistaDBType(parameters[valueIndex], out values[valueIndex]);
        parameterTypes[valueIndex] = vistaDbType;
        Signature signature = this[valueIndex];
        if (signature.SignatureType == SignatureType.Parameter)
        {
          if (outParams == null)
            outParams = new List<OutParameter>();
          outParams.Add(new OutParameter((ParameterSignature) signature, valueIndex));
        }
      }
    }

    protected VistaDBType GetVistaDBType(ParameterInfo param, out VistaDBValue val)
    {
      Type type1 = GetParameterType(param);
      VistaDBType type2 = ConvertType(type1);
      if (!type1.IsSubclassOf(typeof (VistaDBValue)))
        type1 = ConvertType(type2);
      val = (VistaDBValue) type1.GetConstructor(new Type[0]).Invoke(new object[0]);
      return type2;
    }

    protected static object GetTrueValue(VistaDBValue value, ParameterInfo parameter)
    {
      Type parameterType = GetParameterType(parameter);
      if (parameterType.IsSubclassOf(typeof (VistaDBValue)))
        return (object) value;
      if (parameterType.IsPrimitive || parameterType.Equals(typeof (string)))
        return value.Value;
      if (!value.HasValue)
        return Activator.CreateInstance(parameterType);
      return parameterType.GetConstructor(new Type[1]{ value.SystemType }).Invoke(new object[1]{ value.Value });
    }

    protected static object GetTrueValue(object value)
    {
      if (value == null)
        return (object) null;
      if (value is VistaDBValue)
      {
        VistaDBValue vistaDbValue = value as VistaDBValue;
        if (vistaDbValue.HasValue)
          return vistaDbValue.Value;
        return (object) null;
      }
      Type type = value.GetType();
      if (type.IsPrimitive || type.Equals(typeof (string)))
        return value;
      if (type.Equals(typeof (SqlString)))
      {
        SqlString sqlString = (SqlString) value;
        if (sqlString.IsNull)
          return (object) null;
        return (object) sqlString.Value;
      }
      if (type.Equals(typeof (SqlDateTime)))
      {
        SqlDateTime sqlDateTime = (SqlDateTime) value;
        if (sqlDateTime.IsNull)
          return (object) null;
        return (object) sqlDateTime.Value;
      }
      if (type.Equals(typeof (SqlBoolean)))
      {
        SqlBoolean sqlBoolean = (SqlBoolean) value;
        if (sqlBoolean.IsNull)
          return (object) null;
        return (object) sqlBoolean.Value;
      }
      if (type.Equals(typeof (SqlByte)))
      {
        SqlByte sqlByte = (SqlByte) value;
        if (sqlByte.IsNull)
          return (object) null;
        return (object) sqlByte.Value;
      }
      if (type.Equals(typeof (SqlInt16)))
      {
        SqlInt16 sqlInt16 = (SqlInt16) value;
        if (sqlInt16.IsNull)
          return (object) null;
        return (object) sqlInt16.Value;
      }
      if (type.Equals(typeof (SqlInt32)))
      {
        SqlInt32 sqlInt32 = (SqlInt32) value;
        if (sqlInt32.IsNull)
          return (object) null;
        return (object) sqlInt32.Value;
      }
      if (type.Equals(typeof (SqlInt64)))
      {
        SqlInt64 sqlInt64 = (SqlInt64) value;
        if (sqlInt64.IsNull)
          return (object) null;
        return (object) sqlInt64.Value;
      }
      if (type.Equals(typeof (SqlDecimal)))
      {
        SqlDecimal sqlDecimal = (SqlDecimal) value;
        if (sqlDecimal.IsNull)
          return (object) null;
        return (object) sqlDecimal.Value;
      }
      if (type.Equals(typeof (SqlDecimal)))
      {
        SqlBytes sqlBytes = (SqlBytes) value;
        if (sqlBytes.IsNull)
          return (object) null;
        return (object) sqlBytes.Value;
      }
      if (type.Equals(typeof (SqlGuid)))
      {
        SqlGuid sqlGuid = (SqlGuid) value;
        if (sqlGuid.IsNull)
          return (object) null;
        return (object) sqlGuid.Value;
      }
      if (type.Equals(typeof (SqlDouble)))
      {
        SqlDouble sqlDouble = (SqlDouble) value;
        if (sqlDouble.IsNull)
          return (object) null;
        return (object) sqlDouble.Value;
      }
      if (!type.Equals(typeof (SqlSingle)))
        return (object) null;
      SqlSingle sqlSingle = (SqlSingle) value;
      if (sqlSingle.IsNull)
        return (object) null;
      return (object) sqlSingle.Value;
    }

    protected static void SetTrueValue(VistaDBValue dest, object value)
    {
      if (value == null)
      {
        dest.Value = (object) null;
      }
      else
      {
        if (ReferenceEquals((object) dest, value))
          return;
        dest.Value = GetTrueValue(value);
      }
    }

    protected static void SetTrueValue(IParameter dest, object value)
    {
      if (value == null)
      {
        dest.Value = (object) null;
      }
      else
      {
        Type type = value.GetType();
        if (type.Equals(dest.GetType()))
        {
          VistaDBValue vistaDbValue = value as VistaDBValue;
          if (vistaDbValue.IsNull)
            dest.Value = (object) null;
          else
            dest.Value = vistaDbValue.Value;
        }
        else
        {
          if (Nullable.GetUnderlyingType(type) != null)
            return;
          dest.Value = value;
        }
      }
    }

    public override SignatureType OnPrepare()
    {
      PrepareProcedure();
      int num = (int) base.OnPrepare();
      return SignatureType.Expression;
    }

    protected override object ExecuteSubProgram()
    {
      int index1 = 0;
      for (int paramCount = ParamCount; index1 < paramCount; ++index1)
        values[index1].Value = ((IValue) paramValues[index1]).Value;
      object[] parameters1 = new object[values.Length];
      if (values != null)
      {
        ParameterInfo[] parameters2 = method.GetParameters();
        int index2 = 0;
        for (int length = parameters2.Length; index2 < length; ++index2)
          parameters1[index2] = GetTrueValue(values[index2], parameters2[index2]);
      }
      object obj;
      try
      {
        obj = method.Invoke((object) null, parameters1);
      }
      catch (Exception ex)
      {
        throw new VistaDBSQLException(ex, 615, procedureName, lineNo, symbolNo);
      }
      if (obj != null && returnParameter != null)
                SetTrueValue(returnParameter, obj);
      if (parameters1 != null)
      {
        int index2 = 0;
        for (int length = parameters1.Length; index2 < length; ++index2)
                    SetTrueValue(values[index2], parameters1[index2]);
      }
      if (outParams != null)
      {
        ParameterInfo[] parameters2 = method.GetParameters();
        int index2 = 0;
        int count = outParams.Count;
        for (int length = parameters2.Length; index2 < count && index2 < length; ++index2)
        {
                    OutParameter outParam = outParams[index2];
          if ((parameters2[outParam.ValueIndex].Attributes & ParameterAttributes.Out) == ParameterAttributes.Out || outParam.Param.SignatureType == SignatureType.Parameter)
          {
            object val = values[outParam.ValueIndex].Value;
            if (Utils.CompatibleTypes(outParam.Param.DataType, paramValues[index2].Type))
              outParam.Param.SetOutParamValue(val);
          }
        }
      }
      if (fillRow != null)
        return obj;
      if (obj is VistaDBValue)
        return ((VistaDBValue) obj).Value;
      return GetTrueValue(obj);
    }

    private class OutParameter
    {
      public ParameterSignature Param;
      public int ValueIndex;

      public OutParameter(ParameterSignature param, int valueIndex)
      {
        Param = param;
        ValueIndex = valueIndex;
      }
    }
  }
}
