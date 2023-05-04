using System;
using System.Collections;
using System.Reflection;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.VistaDBTypes;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CLRResultSetFunction : CLRStoredProcedure, ITableValuedFunction
  {
    private bool canReset = true;
    private VistaDBType[] resultColumnTypes;
    private string[] resultColumnNames;
    private IEnumerator enumerator;
    private VistaDBValue[] fillParams;

    public CLRResultSetFunction(SQLParser parser, string procedureName)
      : base(parser, procedureName)
    {
      resultColumnTypes = (VistaDBType[]) null;
      resultColumnNames = (string[]) null;
      enumerator = (IEnumerator) null;
      fillParams = (VistaDBValue[]) null;
    }

        private void PrepareFillRowMethod()
    {
      if (fillRow == null)
        throw new VistaDBSQLException(612, procedureName, lineNo, symbolNo);
      ParameterInfo[] parameters = fillRow.GetParameters();
      int length = parameters.Length - 1;
      if (length == 0 || parameters[0].ParameterType != typeof (object))
        throw new VistaDBSQLException(610, procedureName, lineNo, symbolNo);
      fillParams = new VistaDBValue[length + 1];
      resultColumnTypes = new VistaDBType[length];
      resultColumnNames = new string[length];
      for (int index = 0; index < length; ++index)
      {
        ParameterInfo parameterInfo = parameters[index + 1];
        VistaDBValue val;
        resultColumnTypes[index] = GetVistaDBType(parameterInfo, out val);
        resultColumnNames[index] = parameterInfo.Name;
        fillParams[index + 1] = val;
      }
    }

    private void FillRow(IRow row)
    {
      ParameterInfo[] parameters1 = fillRow.GetParameters();
      object[] parameters2 = new object[parameters1.Length];
      parameters2[0] = enumerator.Current;
      int index1 = 1;
      for (int length = parameters1.Length; index1 < length; ++index1)
        parameters2[index1] = GetTrueValue(fillParams[index1], parameters1[index1]);
      try
      {
        fillRow.Invoke((object) null, parameters2);
      }
      catch (Exception ex)
      {
        throw new VistaDBSQLException(ex, 615, procedureName, lineNo, symbolNo);
      }
      int index2 = 1;
      for (int length = parameters1.Length; index2 < length; ++index2)
                SetTrueValue(fillParams[index2], parameters2[index2]);
      int index3 = 0;
      for (int count = row.Count; index3 < count; ++index3)
        ((IValue) row[index3]).Value = fillParams[index3 + 1].Value;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      PrepareFillRowMethod();
      return signatureType;
    }

    public VistaDBType[] GetResultColumnTypes()
    {
      return resultColumnTypes;
    }

    public string[] GetResultColumnNames()
    {
      return resultColumnNames;
    }

    public void Open()
    {
      object resValue;
      if (!PrepareExecute(out resValue))
        return;
      enumerator = !(resValue is IEnumerable) ? (IEnumerator) resValue : ((IEnumerable) resValue).GetEnumerator();
      canReset = true;
      try
      {
        enumerator.Reset();
      }
      catch (NotSupportedException)
            {
        canReset = false;
      }
    }

    public bool First(IRow row)
    {
      if (canReset)
        enumerator.Reset();
      if (!enumerator.MoveNext())
        return false;
      FillRow(row);
      return true;
    }

    public bool GetNextResult(IRow row)
    {
      if (!enumerator.MoveNext())
        return false;
      FillRow(row);
      return true;
    }

    public void Close()
    {
      if (enumerator is IDisposable)
        ((IDisposable) enumerator).Dispose();
      enumerator = (IEnumerator) null;
    }
  }
}
