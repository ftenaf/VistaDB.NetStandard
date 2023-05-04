using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GreaterThanOperator : BinaryCompareOperator
  {
    public GreaterThanOperator(Signature leftOperand, SQLParser parser)
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
        return ((SubQuerySignature) rightOperand).IsValuePresent(leftValue, checkAll, CompareOperation.Greater);
      return leftValue.Compare((IVistaDBColumn) rightValue) > 0;
    }

    protected override CompareOperation GetCompareOperation()
    {
      return CompareOperation.Greater;
    }

    protected override CompareOperation GetRevCompareOperation()
    {
      return CompareOperation.Less;
    }
  }
}
