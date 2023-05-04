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
      alwaysString = parent.Connection.CompareString(text, "||", true) == 0;
      dateOperands = false;
      width = 0;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        needsEvaluation = false;
        IColumn column1 = leftOperand.Execute();
        if (column1.IsNull)
        {
          ((IValue) result).Value = (object) null;
          return result;
        }
        IColumn column2 = rightOperand.Execute();
        if (column2.IsNull)
        {
          ((IValue) result).Value = (object) null;
          return result;
        }
        Convert((IValue) column1, (IValue) leftValue);
        Convert((IValue) column2, (IValue) rightValue);
        if (dateOperands)
          ((IValue) result).Value = GetDateResult();
        else
          ((IValue) result).Value = ((Row.Column) leftValue + (Row.Column) rightValue).Value;
      }
      return result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType;
      if (alwaysString)
      {
        dataType = VistaDBType.NChar;
        leftOperand = ConstantSignature.PrepareAndCheckConstant(leftOperand, dataType);
        rightOperand = ConstantSignature.PrepareAndCheckConstant(rightOperand, dataType);
        if (!Utils.CompatibleTypes(leftOperand.DataType, dataType) || !Utils.CompatibleTypes(rightOperand.DataType, dataType))
          throw new VistaDBSQLException(558, text, lineNo, symbolNo);
        leftValue = CreateColumn(dataType);
        rightValue = CreateColumn(dataType);
        signatureType = leftOperand.SignatureType != SignatureType.Constant || rightOperand.SignatureType != SignatureType.Constant ? SignatureType.Expression : SignatureType.Constant;
      }
      else
        signatureType = ConstantSignature.PreparePlusMinusOperator(ref leftOperand, ref rightOperand, out dataType, out leftValue, out rightValue, out dateOperands, true, text, lineNo, symbolNo);
      width = !Utils.IsCharacterDataType(dataType) ? (Utils.CompareRank(leftOperand.DataType, rightOperand.DataType) < 0 ? rightOperand.GetWidth() : leftOperand.GetWidth()) : leftOperand.GetWidth() + rightOperand.GetWidth();
      return signatureType;
    }

    public override int GetWidth()
    {
      return width;
    }

    private object GetDateResult()
    {
      if (Utils.IsDateDataType(leftValue.Type))
        return (object) ((DateTime) ((IValue) leftValue).Value).AddDays((double) ((IValue) rightValue).Value);
      return (object) ((DateTime) ((IValue) rightValue).Value).AddDays((double) ((IValue) leftValue).Value);
    }
  }
}
