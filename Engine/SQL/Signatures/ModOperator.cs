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
      if (GetIsChanged())
      {
        IColumn column1 = leftOperand.Execute();
        IColumn column2 = rightOperand.Execute();
        if (column1.IsNull || column2.IsNull)
        {
          ((IValue) result).Value = (object) null;
        }
        else
        {
          Convert((IValue) column1, (IValue) leftValue);
          Convert((IValue) column2, (IValue) rightValue);
          ((IValue) result).Value = ((Row.Column) leftValue % (Row.Column) rightValue).Value;
        }
        needsEvaluation = false;
      }
      return result;
    }
  }
}
