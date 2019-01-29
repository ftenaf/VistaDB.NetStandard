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
      this.operand = parser.NextSignature(true, true, priority);
      this.signatureType = SignatureType.Expression;
    }

    public override SignatureType OnPrepare()
    {
      this.operand = ConstantSignature.PrepareAndCheckConstant(this.operand, VistaDBType.Unknown);
      if (this.operand.SignatureType == SignatureType.Constant || this.operand.AlwaysNull)
        return SignatureType.Constant;
      return this.signatureType;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (this.GetType() == signature.GetType())
        return this.operand == ((UnaryOperator) signature).operand;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      this.operand = this.operand.Relink(signature, ref columnCount);
    }

    public override void SetChanged()
    {
      this.needsEvaluation = true;
      this.operand.SetChanged();
    }

    public override void ClearChanged()
    {
      this.needsEvaluation = false;
      this.operand.ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      if (!this.needsEvaluation)
        return this.operand.GetIsChanged();
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      this.operand.GetAggregateFunctions(list);
    }

    public override int ColumnCount
    {
      get
      {
        return this.operand.ColumnCount;
      }
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      return this.operand.HasAggregateFunction(out distinct);
    }

    public override bool AlwaysNull
    {
      get
      {
        return this.operand.AlwaysNull;
      }
    }
  }
}
