using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class EqualOperator : BinaryCompareOperator
  {
    public EqualOperator(Signature leftOperand, SQLParser parser)
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
        return ((SubQuerySignature) rightOperand).IsValuePresent(leftValue, checkAll, CompareOperation.Equal);
      return leftValue.Compare(rightValue) == 0;
    }

    protected override CompareOperation GetCompareOperation()
    {
      return CompareOperation.Equal;
    }

    protected override CompareOperation GetRevCompareOperation()
    {
      return CompareOperation.Equal;
    }

    internal bool GetJoinEqualityColumns(out ColumnSignature leftColumn, out ColumnSignature rightColumn)
    {
      leftColumn = null;
      rightColumn = null;
      if (GetCompareOperation() != CompareOperation.Equal)
        return false;
      leftColumn = leftOperand as ColumnSignature;
      rightColumn = rightOperand as ColumnSignature;
      if (leftColumn != null)
        return rightColumn != null;
      return false;
    }
  }
}
