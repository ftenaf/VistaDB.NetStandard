using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class CrossJoin : Join
  {
    public CrossJoin(IRowSet leftRowSet, IRowSet rightRowSet)
      : base((Signature) null, leftRowSet, rightRowSet)
    {
    }

    protected override bool OnExecuteRowset(ConstraintOperations constraints)
    {
      while (this.leftRowSet.ExecuteRowset(constraints))
      {
        if (this.rightRowSet.ExecuteRowset(constraints))
          return true;
        if (!this.leftRowSet.Next(constraints))
          break;
      }
      return false;
    }
  }
}
