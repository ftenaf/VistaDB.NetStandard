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
      if (this.GetIsChanged())
      {
        IColumn column1 = this.leftOperand.Execute();
        IColumn column2 = this.rightOperand.Execute();
        if (column1.IsNull && column2.IsNull)
        {
          ((IValue) this.result).Value = (object) null;
        }
        else
        {
          this.Convert((IValue) column1, (IValue) this.leftValue);
          this.Convert((IValue) column2, (IValue) this.rightValue);
          ((IValue) this.result).Value = this.GetResult();
        }
        this.needsEvaluation = false;
      }
      return this.result;
    }

    protected virtual object GetResult()
    {
      return ((Row.Column) this.leftValue & (Row.Column) this.rightValue).Value;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = ConstantSignature.PrepareBinaryOperator(ref this.leftOperand, ref this.rightOperand, out this.dataType, true, false, this.text, this.lineNo, this.symbolNo);
      if (!Utils.IsIntegerDataType(this.dataType))
        throw new VistaDBSQLException(558, this.text, this.lineNo, this.symbolNo);
      this.leftValue = this.CreateColumn(this.dataType);
      this.rightValue = this.CreateColumn(this.dataType);
      return signatureType;
    }
  }
}
