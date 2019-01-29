using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class BitwiseNotOperator : UnaryArithmeticOperator
  {
    public BitwiseNotOperator(SQLParser parser)
      : base(parser, -1)
    {
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (!Utils.IsIntegerDataType(this.dataType))
        throw new VistaDBSQLException(558, "~", this.lineNo, this.symbolNo);
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
      {
        ((IValue) this.result).Value = (~(Row.Column) this.operand.Execute()).Value;
        this.needsEvaluation = false;
      }
      return this.result;
    }
  }
}
