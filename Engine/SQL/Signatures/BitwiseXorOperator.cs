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
      return ((Row.Column) this.leftValue ^ (Row.Column) this.rightValue).Value;
    }
  }
}
