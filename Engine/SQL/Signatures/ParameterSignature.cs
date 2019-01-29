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
      this.origParamName = parser.TokenValue.Token;
      this.paramName = this.origParamName.Substring(1);
      this.signatureType = SignatureType.Parameter;
      this.isChanged = true;
      this.optimizable = true;
      this.val = (object) null;
    }

    internal static bool IsParameter(string token)
    {
      if (token.Length > 1 && token[0] == '@')
        return token[1] != '@';
      return false;
    }

    private void EvaluateResult()
    {
      IParameter parameter = this.parent.DoGetParam(this.paramName);
      this.val = parameter.Value;
      this.dataType = parameter.DataType;
      ((IValue) this.result).Value = this.val;
    }

    protected override IColumn InternalExecute()
    {
      if (this.isChanged)
      {
        this.EvaluateResult();
        this.isChanged = false;
      }
      return this.result;
    }

    protected override void OnSimpleExecute()
    {
      if (!this.isChanged)
        return;
      this.EvaluateResult();
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    public override SignatureType OnPrepare()
    {
      IParameter parameter = this.parent.DoGetParam(this.paramName);
      if (parameter == null)
        throw new VistaDBSQLException(616, this.origParamName, this.lineNo, this.symbolNo);
      this.val = parameter.Value;
      this.dataType = parameter.DataType;
      return this.signatureType;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (signature is ParameterSignature)
        return this.parent.Connection.CompareString(this.paramName, ((ParameterSignature) signature).paramName, true) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      this.isChanged = true;
    }

    public override void ClearChanged()
    {
      this.isChanged = false;
    }

    protected override bool InternalGetIsChanged()
    {
      return this.isChanged;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public override int GetWidth()
    {
      if (!Utils.IsCharacterDataType(this.dataType))
        return base.GetWidth();
      if (this.val != null)
        return ((string) this.val).Length;
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
      this.parent.DoSetParam(this.paramName, val, this.dataType, ParameterDirection.Output);
    }
  }
}
