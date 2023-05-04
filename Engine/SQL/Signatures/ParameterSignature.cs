using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ParameterSignature : Signature
  {
    private string origParamName;
    private string paramName;
    private bool isChanged;
    private object val;

    internal static Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new ParameterSignature(parser);
    }

    internal static Signature CreateSignature(string paramName, IParameter parameter)
    {
      ParameterSignature parameterSignature = new ParameterSignature((SQLParser) null);
      parameterSignature.paramName = paramName;
      parameterSignature.dataType = parameter.DataType;
      return (Signature) parameterSignature;
    }

    private ParameterSignature(SQLParser parser)
      : base(parser)
    {
      origParamName = parser.TokenValue.Token;
      paramName = origParamName.Substring(1);
      signatureType = SignatureType.Parameter;
      isChanged = true;
      optimizable = true;
      val = (object) null;
    }

    internal static bool IsParameter(string token)
    {
      if (token.Length > 1 && token[0] == '@')
        return token[1] != '@';
      return false;
    }

    private void EvaluateResult()
    {
      IParameter parameter = parent.DoGetParam(paramName);
      val = parameter.Value;
      dataType = parameter.DataType;
      ((IValue) result).Value = val;
    }

    protected override IColumn InternalExecute()
    {
      if (isChanged)
      {
        EvaluateResult();
        isChanged = false;
      }
      return result;
    }

    protected override void OnSimpleExecute()
    {
      if (!isChanged)
        return;
      EvaluateResult();
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    public override SignatureType OnPrepare()
    {
      IParameter parameter = parent.DoGetParam(paramName);
      if (parameter == null)
        throw new VistaDBSQLException(616, origParamName, lineNo, symbolNo);
      val = parameter.Value;
      dataType = parameter.DataType;
      return signatureType;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature is ParameterSignature)
        return parent.Connection.CompareString(paramName, ((ParameterSignature) signature).paramName, true) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      isChanged = true;
    }

    public override void ClearChanged()
    {
      isChanged = false;
    }

    protected override bool InternalGetIsChanged()
    {
      return isChanged;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public override int GetWidth()
    {
      if (!Utils.IsCharacterDataType(dataType))
        return base.GetWidth();
      if (val != null)
        return ((string) val).Length;
      return 0;
    }

    public override bool AlwaysNull
    {
      get
      {
        return false;
      }
    }

    public override int ColumnCount
    {
      get
      {
        return 0;
      }
    }

    public void SetOutParamValue(object val)
    {
      parent.DoSetParam(paramName, val, dataType, ParameterDirection.Output);
    }
  }
}
