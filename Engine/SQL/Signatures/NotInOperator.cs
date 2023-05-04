namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotInOperator : InOperator
  {
    public NotInOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser)
    {
    }

    protected override bool CompareOperands()
    {
      return !((IValueList) rightOperand).IsValuePresent(leftValue);
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (base.OnOptimize(constrainOperations))
        return constrainOperations.AddLogicalNot();
      return false;
    }
  }
}
