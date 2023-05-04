using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class CrossJoin : Join
  {
    public CrossJoin(IRowSet leftRowSet, IRowSet rightRowSet)
      : base(null, leftRowSet, rightRowSet)
    {
    }

    protected override bool OnExecuteRowset(ConstraintOperations constraints)
    {
      while (leftRowSet.ExecuteRowset(constraints))
      {
        if (rightRowSet.ExecuteRowset(constraints))
          return true;
        if (!leftRowSet.Next(constraints))
          break;
      }
      return false;
    }
  }
}
