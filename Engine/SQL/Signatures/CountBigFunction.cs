namespace VistaDB.Engine.SQL.Signatures
{
  internal class CountBigFunction : AggregateFunction
  {
    public CountBigFunction(SQLParser parser)
      : base(parser, true)
    {
      dataType = VistaDBType.BigInt;
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
      return 0L;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      return all || newVal != null ? 1L : 0L;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (!all && newVal == null)
        return val;
      return (long)val + 1L;
    }

    protected override object InternalFinishGroup()
    {
      return val;
    }
  }
}
