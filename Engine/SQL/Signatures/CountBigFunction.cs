namespace VistaDB.Engine.SQL.Signatures
{
  internal class CountBigFunction : AggregateFunction
  {
    public CountBigFunction(SQLParser parser)
      : base(parser, true)
    {
      this.dataType = VistaDBType.BigInt;
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
      return (object) 0L;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      return (object) (this.all || newVal != null ? 1L : 0L);
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (!this.all && newVal == null)
        return this.val;
      return (object) ((long) this.val + 1L);
    }

    protected override object InternalFinishGroup()
    {
      return this.val;
    }
  }
}
