namespace VistaDB.Engine.SQL.Signatures
{
  internal class CountFunction : AggregateFunction
  {
    public CountFunction(SQLParser parser)
      : base(parser, true)
    {
      dataType = VistaDBType.Int;
    }

    protected override void InternalSerialize(ref object serObj)
    {
      serObj = val;
    }

    protected override void InternalDeserialize(object serObj)
    {
      val = serObj;
    }

    protected override object InternalCreateEmptyResult()
    {
      return 0;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      return all || newVal != null ? 1 : 0;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (!all && newVal == null)
        return val;
      SelectStatement parent = this.parent as SelectStatement;
      if (parent == null || parent.AggregateFunctionCount != 1 || (parent.HasWhereClause || parent.HasGroupByClause) || (distinct || !(parent.RowSet is NativeSourceTable) && !(parent.RowSet is CrossJoin)))
        return (int)val + 1;
      int sourceTableCount = parent.SourceTableCount;
      long num = 1;
      for (int index = 0; index < sourceTableCount; ++index)
      {
        long optimizedRowCount = parent.GetSourceTable(index).GetOptimizedRowCount(!(expression != null) || expression.SignatureType != SignatureType.Column ? null : ((ColumnSignature) expression).ColumnName);
        if (optimizedRowCount < 0L)
          return (int)val + 1;
        num *= optimizedRowCount;
      }
      countOptimized = true;
      return (int)num;
    }

    protected override object InternalFinishGroup()
    {
      return val;
    }
  }
}
