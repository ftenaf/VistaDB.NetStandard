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
      while (leftRowSet.ExecuteRowset(constraints))
      {
        if (ExecuteRightRowSet(constraints))
          return true;
        if (!leftRowSet.Next(constraints))
          break;
      }
      return false;
    }
  }
}
