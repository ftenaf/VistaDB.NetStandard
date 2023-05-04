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
      if (rightOperandIsSubQuery)
        return ((SubQuerySignature) rightOperand).IsValuePresent(leftValue, checkAll, CompareOperation.NotEqual);
      return leftValue.Compare((IVistaDBColumn) rightValue) != 0;
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
