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
      this.dateOperands = false;
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
          if (this.dateOperands)
            ((IValue) this.result).Value = this.GetDateResult();
          else
            ((IValue) this.result).Value = ((Row.Column) this.leftValue - (Row.Column) this.rightValue).Value;
        }
        this.needsEvaluation = false;
      }
      return this.result;
    }

    private object GetDateResult()
    {
      return (object) ((DateTime) ((IValue) this.leftValue).Value).AddDays(-(double) ((IValue) this.rightValue).Value);
    }

    public override SignatureType OnPrepare()
    {
      return ConstantSignature.PreparePlusMinusOperator(ref this.leftOperand, ref this.rightOperand, out this.dataType, out this.leftValue, out this.rightValue, out this.dateOperands, false, this.text, this.lineNo, this.symbolNo);
    }
  }
}
