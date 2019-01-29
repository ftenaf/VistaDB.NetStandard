using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GreaterOrEqualOperator : BinaryCompareOperator
  {
    public GreaterOrEqualOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser)
    {
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.CalcOptimizeLevel();
      return signatureType;
    }

    protected override bool CompareOperands()
    {
      if (this.rightOperandIsSubQuery)
        return ((SubQuerySignature) this.rightOperand).IsValuePresent(this.leftValue, this.checkAll, CompareOperation.GreaterOrEqual);
      return this.leftValue.Compare((IVistaDBColumn) this.rightValue) >= 0;
    }

    protected override CompareOperation GetCompareOperation()
    {
      return CompareOperation.GreaterOrEqual;
    }

    protected override CompareOperation GetRevCompareOperation()
    {
      return CompareOperation.LessOrEqual;
    }
  }
}
