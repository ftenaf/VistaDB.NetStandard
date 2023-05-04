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
      CalcOptimizeLevel();
      return signatureType;
    }

    protected override bool CompareOperands()
    {
      if (rightOperandIsSubQuery)
        return ((SubQuerySignature) rightOperand).IsValuePresent(leftValue, checkAll, CompareOperation.GreaterOrEqual);
      return leftValue.Compare((IVistaDBColumn) rightValue) >= 0;
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
