using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class MaxFunction : AggregateFunction
  {
    private IColumn tempValue;

    public MaxFunction(SQLParser parser)
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
      return (object) null;
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
      ((IValue) result).Value = val;
      ((IValue) tempValue).Value = newVal;
      if (tempValue.Compare((IVistaDBColumn) result) <= 0)
        return val;
      return newVal;
    }

    protected override object InternalFinishGroup()
    {
      return val;
    }
  }
}
