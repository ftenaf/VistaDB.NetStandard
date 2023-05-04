using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LessThanOperator : BinaryCompareOperator
  {
    public LessThanOperator(Signature leftOperand, SQLParser parser)
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
        return ((SubQuerySignature) rightOperand).IsValuePresent(leftValue, checkAll, CompareOperation.Less);
      return leftValue.Compare(rightValue) < 0;
    }

    protected override CompareOperation GetCompareOperation()
    {
      return CompareOperation.Less;
    }

    protected override CompareOperation GetRevCompareOperation()
    {
      return CompareOperation.Greater;
    }
  }
}
