using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnaryMinusOperator : UnaryArithmeticOperator
  {
    public UnaryMinusOperator(SQLParser parser)
      : base(parser, 1)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        Row.Column column = -(Row.Column) this.operand.Execute();
        if (this.result == null)
          this.result = this.CreateColumn(column.Type);
        ((IValue) this.result).Value = column.Value;
        this.needsEvaluation = false;
      }
      return this.result;
    }
  }
}
