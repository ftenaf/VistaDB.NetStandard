using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NotLikeOperator : LikeOperator
  {
    public NotLikeOperator(Signature expression, SQLParser parser)
      : base(expression, parser)
    {
    }

    protected override bool Compare(string matchExpr)
    {
      return !base.Compare(matchExpr);
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      int chunkCount;
      if (!this.CreatePattern() || this.finder.GetOptimizationLevel(out chunkCount) != OptimizationLevel.Full)
        return false;
      Signature low;
      Signature high;
      this.finder.GetOptimizationScopeSignatures(this.parent, out low, out high);
      if (chunkCount > 1)
      {
        if (constrainOperations.AddLogicalBetween((ColumnSignature) this.expression, low, high, false))
          return constrainOperations.AddLogicalNot();
        return false;
      }
      if (constrainOperations.AddLogicalCompare(this.expression, low, CompareOperation.Equal, CompareOperation.Equal, false))
        return constrainOperations.AddLogicalNot();
      return false;
    }
  }
}
