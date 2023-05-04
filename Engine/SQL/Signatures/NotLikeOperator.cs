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
      if (!CreatePattern() || finder.GetOptimizationLevel(out chunkCount) != OptimizationLevel.Full)
        return false;
      Signature low;
      Signature high;
      finder.GetOptimizationScopeSignatures(parent, out low, out high);
      if (chunkCount > 1)
      {
        if (constrainOperations.AddLogicalBetween((ColumnSignature) expression, low, high, false))
          return constrainOperations.AddLogicalNot();
        return false;
      }
      if (constrainOperations.AddLogicalCompare(expression, low, CompareOperation.Equal, CompareOperation.Equal, false))
        return constrainOperations.AddLogicalNot();
      return false;
    }
  }
}
