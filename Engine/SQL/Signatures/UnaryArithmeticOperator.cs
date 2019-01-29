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
      if (this.AlwaysNull)
      {
        this.dataType = VistaDBType.BigInt;
        return SignatureType.Constant;
      }
      if (!Utils.IsNumericDataType(this.operand.DataType))
        throw new VistaDBSQLException(558, this.text, this.lineNo, this.symbolNo);
      this.dataType = this.operand.DataType;
      return signatureType;
    }
  }
}
