using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseAndOperator : BinaryOperator
  {
    public BitwiseAndOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 1)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        IColumn column1 = leftOperand.Execute();
        IColumn column2 = rightOperand.Execute();
        if (column1.IsNull && column2.IsNull)
        {
                    result.Value = null;
        }
        else
        {
          Convert(column1, leftValue);
          Convert(column2, rightValue);
                    result.Value = GetResult();
        }
        needsEvaluation = false;
      }
      return result;
    }

    protected virtual object GetResult()
    {
      return ((Row.Column) leftValue & (Row.Column) rightValue).Value;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = ConstantSignature.PrepareBinaryOperator(ref leftOperand, ref rightOperand, out dataType, true, false, text, lineNo, symbolNo);
      if (!Utils.IsIntegerDataType(dataType))
        throw new VistaDBSQLException(558, text, lineNo, symbolNo);
      leftValue = CreateColumn(dataType);
      rightValue = CreateColumn(dataType);
      return signatureType;
    }
  }
}
