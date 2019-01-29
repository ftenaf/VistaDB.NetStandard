using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ModOperator : ArithmeticP2Operator
  {
    public ModOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 0)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        IColumn column1 = this.leftOperand.Execute();
        IColumn column2 = this.rightOperand.Execute();
        if (column1.IsNull || column2.IsNull)
        {
          ((IValue) this.result).Value = (object) null;
        }
        else
        {
          this.Convert((IValue) column1, (IValue) this.leftValue);
          this.Convert((IValue) column2, (IValue) this.rightValue);
          ((IValue) this.result).Value = ((Row.Column) this.leftValue % (Row.Column) this.rightValue).Value;
        }
        this.needsEvaluation = false;
      }
      return this.result;
    }
  }
}
