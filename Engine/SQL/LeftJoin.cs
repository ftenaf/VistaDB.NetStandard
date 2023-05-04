using VistaDB.DDA;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class LeftJoin : Join
  {
    public LeftJoin(Signature signature, IRowSet leftRowSet, IRowSet rightRowSet)
      : base(signature, leftRowSet, rightRowSet)
    {
    }

    public override IRowSet PrepareTables(IVistaDBTableNameCollection tableNames, IViewList views, TableCollection tableList, bool alwaysAllowNull, ref int tableIndex)
    {
      leftRowSet = leftRowSet.PrepareTables(tableNames, views, tableList, alwaysAllowNull, ref tableIndex);
      rightRowSet = rightRowSet.PrepareTables(tableNames, views, tableList, true, ref tableIndex);
      return (IRowSet) this;
    }

    protected override bool OnExecuteRowset(ConstraintOperations constraints)
    {
      do
      {
        bool rowUpdated = leftRowSet.RowUpdated;
        if (leftRowSet.ExecuteRowset(constraints))
        {
          if (ExecuteRightRowSet(constraints))
            return true;
          if (rowUpdated)
          {
            rightRowSet.MarkRowNotAvailable();
            return true;
          }
        }
        else
          break;
      }
      while (leftRowSet.Next(constraints));
      return false;
    }

    public override bool OuterRow
    {
      get
      {
        return leftRowSet.RowAvailable ^ rightRowSet.RowAvailable;
      }
    }
  }
}
