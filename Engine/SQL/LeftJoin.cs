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
      this.leftRowSet = this.leftRowSet.PrepareTables(tableNames, views, tableList, alwaysAllowNull, ref tableIndex);
      this.rightRowSet = this.rightRowSet.PrepareTables(tableNames, views, tableList, true, ref tableIndex);
      return (IRowSet) this;
    }

    protected override bool OnExecuteRowset(ConstraintOperations constraints)
    {
      do
      {
        bool rowUpdated = this.leftRowSet.RowUpdated;
        if (this.leftRowSet.ExecuteRowset(constraints))
        {
          if (this.ExecuteRightRowSet(constraints))
            return true;
          if (rowUpdated)
          {
            this.rightRowSet.MarkRowNotAvailable();
            return true;
          }
        }
        else
          break;
      }
      while (this.leftRowSet.Next(constraints));
      return false;
    }

    public override bool OuterRow
    {
      get
      {
        return this.leftRowSet.RowAvailable ^ this.rightRowSet.RowAvailable;
      }
    }
  }
}
