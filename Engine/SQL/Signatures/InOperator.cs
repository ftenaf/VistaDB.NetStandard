using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class InOperator : BinaryCompareOperator
  {
    public InOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser)
    {
      parser.SkipToken(true);
      parser.ExpectedExpression("(");
      parser.SkipToken(true);
      if (SubQuerySignature.IsSubQuery(parser.TokenValue.Token))
        rightOperand = SubQuerySignature.CreateSignature(parser);
      else
        rightOperand = ValueListSignature.CreateSignature(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
      rightOperandIsSubQuery = true;
    }

    protected override void DoParseRightOperand(SQLParser parser, int priority)
    {
    }

    public override SignatureType OnPrepare()
    {
      leftOperand = ConstantSignature.PrepareAndCheckConstant(leftOperand, VistaDBType.Unknown);
      int num = (int) rightOperand.OnPrepare();
      operandType = leftOperand.DataType;
      optimizable = rightOperand.SignatureType == SignatureType.Constant;
      if (leftOperand.AlwaysNull || rightOperand.AlwaysNull || leftOperand.SignatureType == SignatureType.Constant && leftOperand.SignatureType == SignatureType.Constant)
        return SignatureType.Constant;
      return signatureType;
    }

    protected override bool CompareOperands()
    {
      if (rightOperand.DataType == VistaDBType.Unknown)
        throw new VistaDBSQLException(637, Text, LineNo, SymbolNo);
      return ((IValueList) rightOperand).IsValuePresent(leftValue);
    }

    protected override CompareOperation GetCompareOperation()
    {
      return CompareOperation.Equal;
    }

    protected override CompareOperation GetRevCompareOperation()
    {
      return CompareOperation.Equal;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      ValueListSignature rightOperand1 = (ValueListSignature) rightOperand;
      if (rightOperand1.Count > 3)
        return false;
      int num = 0;
      foreach (Signature rightOperand2 in rightOperand1)
      {
        if (!constrainOperations.AddLogicalCompare(leftOperand, rightOperand2, CompareOperation.Equal, CompareOperation.Equal, false) || num > 0 && !constrainOperations.AddLogicalOr())
          return false;
        ++num;
      }
      return true;
    }
  }
}
