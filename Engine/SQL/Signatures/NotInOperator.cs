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
      return !((IValueList) this.rightOperand).IsValuePresent(this.leftValue);
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (base.OnOptimize(constrainOperations))
        return constrainOperations.AddLogicalNot();
      return false;
    }
  }
}
