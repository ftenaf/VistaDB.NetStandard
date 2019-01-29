using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class PlusOperator : BinaryOperator
  {
    private bool alwaysString;
    private bool dateOperands;
    private int width;

    public PlusOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 1)
    {
      this.alwaysString = this.parent.Connection.CompareString(this.text, "||", true) == 0;
      this.dateOperands = false;
      this.width = 0;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        this.needsEvaluation = false;
        IColumn column1 = this.leftOperand.Execute();
        if (column1.IsNull)
        {
          ((IValue) this.result).Value = (object) null;
          return this.result;
        }
        IColumn column2 = this.rightOperand.Execute();
        if (column2.IsNull)
        {
          ((IValue) this.result).Value = (object) null;
          return this.result;
        }
        this.Convert((IValue) column1, (IValue) this.leftValue);
        this.Convert((IValue) column2, (IValue) this.rightValue);
        if (this.dateOperands)
          ((IValue) this.result).Value = this.GetDateResult();
        else
          ((IValue) this.result).Value = ((Row.Column) this.leftValue + (Row.Column) this.rightValue).Value;
      }
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType;
      if (this.alwaysString)
      {
        this.dataType = VistaDBType.NChar;
        this.leftOperand = ConstantSignature.PrepareAndCheckConstant(this.leftOperand, this.dataType);
        this.rightOperand = ConstantSignature.PrepareAndCheckConstant(this.rightOperand, this.dataType);
        if (!Utils.CompatibleTypes(this.leftOperand.DataType, this.dataType) || !Utils.CompatibleTypes(this.rightOperand.DataType, this.dataType))
          throw new VistaDBSQLException(558, this.text, this.lineNo, this.symbolNo);
        this.leftValue = this.CreateColumn(this.dataType);
        this.rightValue = this.CreateColumn(this.dataType);
        signatureType = this.leftOperand.SignatureType != SignatureType.Constant || this.rightOperand.SignatureType != SignatureType.Constant ? SignatureType.Expression : SignatureType.Constant;
      }
      else
        signatureType = ConstantSignature.PreparePlusMinusOperator(ref this.leftOperand, ref this.rightOperand, out this.dataType, out this.leftValue, out this.rightValue, out this.dateOperands, true, this.text, this.lineNo, this.symbolNo);
      this.width = !Utils.IsCharacterDataType(this.dataType) ? (Utils.CompareRank(this.leftOperand.DataType, this.rightOperand.DataType) < 0 ? this.rightOperand.GetWidth() : this.leftOperand.GetWidth()) : this.leftOperand.GetWidth() + this.rightOperand.GetWidth();
      return signatureType;
    }

    public override int GetWidth()
    {
      return this.width;
    }

    private object GetDateResult()
    {
      if (Utils.IsDateDataType(this.leftValue.Type))
        return (object) ((DateTime) ((IValue) this.leftValue).Value).AddDays((double) ((IValue) this.rightValue).Value);
      return (object) ((DateTime) ((IValue) this.rightValue).Value).AddDays((double) ((IValue) this.leftValue).Value);
    }
  }
}
