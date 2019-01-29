namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotBetweenOperator : BetweenOperator
  {
    public NotBetweenOperator(Signature expression, SQLParser parser)
      : base(expression, parser)
    {
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (this.expression.SignatureType == SignatureType.Column && constrainOperations.AddLogicalBetween((ColumnSignature) this.expression, this.beginExpression, this.endExpression, false))
        return constrainOperations.AddLogicalNot();
      return false;
    }

    protected override bool ProcessResult(bool result)
    {
      return !result;
    }
  }
}
