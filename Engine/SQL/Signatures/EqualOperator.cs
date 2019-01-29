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
      this.CalcOptimizeLevel();
      return signatureType;
    }

    protected override bool CompareOperands()
    {
      if (this.rightOperandIsSubQuery)
        return ((SubQuerySignature) this.rightOperand).IsValuePresent(this.leftValue, this.checkAll, CompareOperation.Equal);
      return this.leftValue.Compare((IVistaDBColumn) this.rightValue) == 0;
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
      leftColumn = (ColumnSignature) null;
      rightColumn = (ColumnSignature) null;
      if (this.GetCompareOperation() != CompareOperation.Equal)
        return false;
      leftColumn = this.leftOperand as ColumnSignature;
      rightColumn = this.rightOperand as ColumnSignature;
      if ((Signature) leftColumn != (Signature) null)
        return (Signature) rightColumn != (Signature) null;
      return false;
    }
  }
}
