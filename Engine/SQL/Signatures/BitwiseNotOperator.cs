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
      if (!Utils.IsIntegerDataType(dataType))
        throw new VistaDBSQLException(558, "~", lineNo, symbolNo);
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
      {
        ((IValue) result).Value = (~(Row.Column) operand.Execute()).Value;
        needsEvaluation = false;
      }
      return result;
    }
  }
}
