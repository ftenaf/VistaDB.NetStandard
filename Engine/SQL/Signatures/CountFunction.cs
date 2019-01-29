namespace VistaDB.Engine.SQL.Signatures
{
  internal class CountFunction : AggregateFunction
  {
    public CountFunction(SQLParser parser)
      : base(parser, true)
    {
      this.dataType = VistaDBType.Int;
    }

    protected override void InternalSerialize(ref object serObj)
    {
      serObj = this.val;
    }

    protected override void InternalDeserialize(object serObj)
    {
      this.val = serObj;
    }

    protected override object InternalCreateEmptyResult()
    {
      return (object) 0;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      return (object) (this.all || newVal != null ? 1 : 0);
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (!this.all && newVal == null)
        return this.val;
      SelectStatement parent = this.parent as SelectStatement;
      if (parent == null || parent.AggregateFunctionCount != 1 || (parent.HasWhereClause || parent.HasGroupByClause) || (this.distinct || !(parent.RowSet is NativeSourceTable) && !(parent.RowSet is CrossJoin)))
        return (object) ((int) this.val + 1);
      int sourceTableCount = parent.SourceTableCount;
      long num = 1;
      for (int index = 0; index < sourceTableCount; ++index)
      {
        long optimizedRowCount = parent.GetSourceTable(index).GetOptimizedRowCount(!(this.expression != (Signature) null) || this.expression.SignatureType != SignatureType.Column ? (string) null : ((ColumnSignature) this.expression).ColumnName);
        if (optimizedRowCount < 0L)
          return (object) ((int) this.val + 1);
        num *= optimizedRowCount;
      }
      this.countOptimized = true;
      return (object) (int) num;
    }

    protected override object InternalFinishGroup()
    {
      return this.val;
    }
  }
}
