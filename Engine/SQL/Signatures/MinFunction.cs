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
      this.dataType = this.expression.DataType;
      this.tempValue = this.CreateColumn(this.dataType);
      this.result = this.CreateColumn(this.dataType);
      return signatureType;
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
      return (object) null;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      return newVal;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return this.val;
      if (this.val == null)
        return newVal;
      ((IValue) this.result).Value = this.val;
      ((IValue) this.tempValue).Value = newVal;
      if (this.tempValue.Compare((IVistaDBColumn) this.result) >= 0)
        return this.val;
      return newVal;
    }

    protected override object InternalFinishGroup()
    {
      return this.val;
    }
  }
}
