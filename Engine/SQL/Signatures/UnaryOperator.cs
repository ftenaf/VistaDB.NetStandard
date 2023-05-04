using System.Collections.Generic;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class UnaryOperator : Signature
  {
    protected bool needsEvaluation = true;
    protected Signature operand;

    public UnaryOperator(SQLParser parser, int priority)
      : base(parser)
    {
      operand = parser.NextSignature(true, true, priority);
      signatureType = SignatureType.Expression;
    }

    public override SignatureType OnPrepare()
    {
      operand = ConstantSignature.PrepareAndCheckConstant(operand, VistaDBType.Unknown);
      if (operand.SignatureType == SignatureType.Constant || operand.AlwaysNull)
        return SignatureType.Constant;
      return signatureType;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (GetType() == signature.GetType())
        return operand == ((UnaryOperator) signature).operand;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      operand = operand.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      needsEvaluation = true;
      operand.SetChanged();
    }

    public override void ClearChanged()
    {
      needsEvaluation = false;
      operand.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!needsEvaluation)
        return operand.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      operand.GetAggregateFunctions(list);
    }

    public override int ColumnCount
    {
      get
      {
        return operand.ColumnCount;
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      return operand.HasAggregateFunction(out distinct);
    }

    public override bool AlwaysNull
    {
      get
      {
        return operand.AlwaysNull;
      }
    }
  }
}
