using VistaDB.Diagnostic;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class UnaryArithmeticOperator : UnaryOperator
  {
    public UnaryArithmeticOperator(SQLParser parser, int priority)
      : base(parser, priority)
    {
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (AlwaysNull)
      {
        dataType = VistaDBType.BigInt;
        return SignatureType.Constant;
      }
      if (!Utils.IsNumericDataType(operand.DataType))
        throw new VistaDBSQLException(558, text, lineNo, symbolNo);
      dataType = operand.DataType;
      return signatureType;
    }
  }
}
