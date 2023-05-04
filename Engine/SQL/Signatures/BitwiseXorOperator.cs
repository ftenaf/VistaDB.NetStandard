using VistaDB.Engine.Core;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseXorOperator : BitwiseAndOperator
  {
    public BitwiseXorOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser)
    {
    }

    protected override object GetResult()
    {
      return ((Row.Column) leftValue ^ (Row.Column) rightValue).Value;
    }
  }
}
