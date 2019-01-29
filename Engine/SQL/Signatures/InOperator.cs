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
        this.rightOperand = SubQuerySignature.CreateSignature(parser);
      else
        this.rightOperand = ValueListSignature.CreateSignature(parser);
      parser.ExpectedExpression(")");
      parser.SkipToken(false);
      this.rightOperandIsSubQuery = true;
    }

    protected override void DoParseRightOperand(SQLParser parser, int priority)
    {
    }

    public override SignatureType OnPrepare()
    {
      this.leftOperand = ConstantSignature.PrepareAndCheckConstant(this.leftOperand, VistaDBType.Unknown);
      int num = (int) this.rightOperand.OnPrepare();
      this.operandType = this.leftOperand.DataType;
      this.optimizable = this.rightOperand.SignatureType == SignatureType.Constant;
      if (this.leftOperand.AlwaysNull || this.rightOperand.AlwaysNull || this.leftOperand.SignatureType == SignatureType.Constant && this.leftOperand.SignatureType == SignatureType.Constant)
        return SignatureType.Constant;
      return this.signatureType;
    }

    protected override bool CompareOperands()
    {
      if (this.rightOperand.DataType == VistaDBType.Unknown)
        throw new VistaDBSQLException(637, this.Text, this.LineNo, this.SymbolNo);
      return ((IValueList) this.rightOperand).IsValuePresent(this.leftValue);
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
      ValueListSignature rightOperand1 = (ValueListSignature) this.rightOperand;
      if (rightOperand1.Count > 3)
        return false;
      int num = 0;
      foreach (Signature rightOperand2 in rightOperand1)
      {
        if (!constrainOperations.AddLogicalCompare(this.leftOperand, rightOperand2, CompareOperation.Equal, CompareOperation.Equal, false) || num > 0 && !constrainOperations.AddLogicalOr())
          return false;
        ++num;
      }
      return true;
    }
  }
}
