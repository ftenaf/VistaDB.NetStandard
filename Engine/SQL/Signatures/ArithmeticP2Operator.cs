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
      SignatureType signatureType = ConstantSignature.PrepareBinaryOperator(ref leftOperand, ref rightOperand, out dataType, true, true, text, lineNo, symbolNo);
      leftValue = CreateColumn(dataType);
      rightValue = CreateColumn(dataType);
      return signatureType;
    }
  }
}
