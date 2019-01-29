namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class ArithmeticP2Operator : BinaryOperator
  {
    public ArithmeticP2Operator(Signature leftOperand, SQLParser parser, int priority)
      : base(leftOperand, parser, priority)
    {
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = ConstantSignature.PrepareBinaryOperator(ref this.leftOperand, ref this.rightOperand, out this.dataType, true, true, this.text, this.lineNo, this.symbolNo);
      this.leftValue = this.CreateColumn(this.dataType);
      this.rightValue = this.CreateColumn(this.dataType);
      return signatureType;
    }
  }
}
