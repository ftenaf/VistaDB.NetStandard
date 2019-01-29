using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnaryPlusOperator : UnaryArithmeticOperator
  {
    public UnaryPlusOperator(SQLParser parser)
      : base(parser, 1)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        ((IValue) this.result).Value = ((IValue) this.operand.Execute()).Value;
        this.needsEvaluation = false;
      }
      return this.result;
    }
  }
}
