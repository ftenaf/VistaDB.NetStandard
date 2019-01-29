using System;
using VistaDB.DDA;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class OuterJoin : Join
  {
    public OuterJoin(Signature signature, IRowSet leftRowSet, IRowSet rightRowSet)
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
      throw new NotImplementedException();
    }
  }
}
