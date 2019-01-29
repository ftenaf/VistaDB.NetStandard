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
      this.resultColumnTypes = (VistaDBType[]) null;
      this.resultColumnNames = (string[]) null;
      this.enumerator = (IEnumerator) null;
      this.fillParams = (VistaDBValue[]) null;
    }

    private void PreparePipedResult()
    {
    }

    private void PrepareFillRowMethod()
    {
      if (this.fillRow == null)
        throw new VistaDBSQLException(612, this.procedureName, this.lineNo, this.symbolNo);
      ParameterInfo[] parameters = this.fillRow.GetParameters();
      int length = parameters.Length - 1;
      if (length == 0 || parameters[0].ParameterType != typeof (object))
        throw new VistaDBSQLException(610, this.procedureName, this.lineNo, this.symbolNo);
      this.fillParams = new VistaDBValue[length + 1];
      this.resultColumnTypes = new VistaDBType[length];
      this.resultColumnNames = new string[length];
      for (int index = 0; index < length; ++index)
      {
        ParameterInfo parameterInfo = parameters[index + 1];
        VistaDBValue val;
        this.resultColumnTypes[index] = this.GetVistaDBType(parameterInfo, out val);
        this.resultColumnNames[index] = parameterInfo.Name;
        this.fillParams[index + 1] = val;
      }
    }

    private void FillRow(IRow row)
    {
      ParameterInfo[] parameters1 = this.fillRow.GetParameters();
      object[] parameters2 = new object[parameters1.Length];
      parameters2[0] = this.enumerator.Current;
      int index1 = 1;
      for (int length = parameters1.Length; index1 < length; ++index1)
        parameters2[index1] = CLRStoredProcedure.GetTrueValue(this.fillParams[index1], parameters1[index1]);
      try
      {
        this.fillRow.Invoke((object) null, parameters2);
      }
      catch (Exception ex)
      {
        throw new VistaDBSQLException(ex, 615, this.procedureName, this.lineNo, this.symbolNo);
      }
      int index2 = 1;
      for (int length = parameters1.Length; index2 < length; ++index2)
        CLRStoredProcedure.SetTrueValue(this.fillParams[index2], parameters2[index2]);
      int index3 = 0;
      for (int count = row.Count; index3 < count; ++index3)
        ((IValue) row[index3]).Value = this.fillParams[index3 + 1].Value;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.PrepareFillRowMethod();
      return signatureType;
    }

    public VistaDBType[] GetResultColumnTypes()
    {
      return this.resultColumnTypes;
    }

    public string[] GetResultColumnNames()
    {
      return this.resultColumnNames;
    }

    public void Open()
    {
      object resValue;
      if (!this.PrepareExecute(out resValue))
        return;
      this.enumerator = !(resValue is IEnumerable) ? (IEnumerator) resValue : ((IEnumerable) resValue).GetEnumerator();
      this.canReset = true;
      try
      {
        this.enumerator.Reset();
      }
      catch (NotSupportedException ex)
      {
        this.canReset = false;
      }
    }

    public bool First(IRow row)
    {
      if (this.canReset)
        this.enumerator.Reset();
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row);
      return true;
    }

    public bool GetNextResult(IRow row)
    {
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row);
      return true;
    }

    public void Close()
    {
      if (this.enumerator is IDisposable)
        ((IDisposable) this.enumerator).Dispose();
      this.enumerator = (IEnumerator) null;
    }
  }
}
