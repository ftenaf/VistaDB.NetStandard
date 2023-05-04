using System;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinusOperator : BinaryOperator
  {
    private bool dateOperands;

    public MinusOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 1)
    {
      dateOperands = false;
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
          if (dateOperands)
            ((IValue) result).Value = GetDateResult();
          else
            ((IValue) result).Value = ((Row.Column) leftValue - (Row.Column) rightValue).Value;
        }
        needsEvaluation = false;
      }
      return result;
    }

    private object GetDateResult()
    {
      return (object) ((DateTime) ((IValue) leftValue).Value).AddDays(-(double) ((IValue) rightValue).Value);
    }

    public override SignatureType OnPrepare()
    {
      return ConstantSignature.PreparePlusMinusOperator(ref leftOperand, ref rightOperand, out dataType, out leftValue, out rightValue, out dateOperands, false, text, lineNo, symbolNo);
    }
  }
}
