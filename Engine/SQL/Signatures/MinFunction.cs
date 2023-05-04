using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinFunction : AggregateFunction
  {
    private IColumn tempValue;

    public MinFunction(SQLParser parser)
      : base(parser, false)
    {
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = expression.DataType;
      tempValue = CreateColumn(dataType);
      result = CreateColumn(dataType);
      return signatureType;
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
      return null;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      return newVal;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return val;
      if (val == null)
        return newVal;
            result.Value = val;
            tempValue.Value = newVal;
      if (tempValue.Compare(result) >= 0)
        return val;
      return newVal;
    }

    protected override object InternalFinishGroup()
    {
      return val;
    }
  }
}
