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
      if (expression.SignatureType == SignatureType.Column && constrainOperations.AddLogicalBetween((ColumnSignature) expression, beginExpression, endExpression, false))
        return constrainOperations.AddLogicalNot();
      return false;
    }

    protected override bool ProcessResult(bool result)
    {
      return !result;
    }
  }
}
