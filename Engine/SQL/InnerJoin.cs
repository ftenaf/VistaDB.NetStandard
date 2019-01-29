using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class InnerJoin : Join
  {
    public InnerJoin(Signature signature, IRowSet leftRowset, IRowSet rightRowSet)
      : base(signature, leftRowset, rightRowSet)
    {
    }

    protected override bool OnExecuteRowset(ConstraintOperations constraints)
    {
      while (this.leftRowSet.ExecuteRowset(constraints))
      {
        if (this.ExecuteRightRowSet(constraints))
          return true;
        if (!this.leftRowSet.Next(constraints))
          break;
      }
      return false;
    }
  }
}
