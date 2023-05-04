using VistaDB.Engine.Core;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseOrOperator : BitwiseAndOperator
  {
    public BitwiseOrOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser)
    {
    }

    protected override object GetResult()
    {
      return ((Row.Column) leftValue | (Row.Column) rightValue).Value;
    }
  }
}
