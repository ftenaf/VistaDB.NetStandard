using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DivideOperator : ArithmeticP2Operator
  {
    public DivideOperator(Signature leftOperand, SQLParser parser)
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
                    result.Value = null;
        }
        else
        {
          Convert(column1, leftValue);
          Convert(column2, rightValue);
                    result.Value = ((Row.Column) leftValue / (Row.Column) rightValue).Value;
        }
        needsEvaluation = false;
      }
      return result;
    }
  }
}
