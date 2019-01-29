using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotEqualOperator : EqualOperator
  {
    public NotEqualOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser)
    {
    }

    protected override bool CompareOperands()
    {
      if (this.rightOperandIsSubQuery)
        return ((SubQuerySignature) this.rightOperand).IsValuePresent(this.leftValue, this.checkAll, CompareOperation.NotEqual);
      return this.leftValue.Compare((IVistaDBColumn) this.rightValue) != 0;
    }

    protected override CompareOperation GetCompareOperation()
    {
      return CompareOperation.NotEqual;
    }

    protected override CompareOperation GetRevCompareOperation()
    {
      return CompareOperation.NotEqual;
    }
  }
}
