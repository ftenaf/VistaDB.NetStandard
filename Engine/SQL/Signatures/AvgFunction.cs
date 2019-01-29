using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class AvgFunction : AggregateFunction
  {
    private IColumn srcValue;
    private IColumn dstValue;
    private IColumn tmpValue;
    private long count;

    public AvgFunction(SQLParser parser)
      : base(parser, false)
    {
      this.srcValue = (IColumn) null;
      this.dstValue = (IColumn) null;
      this.tmpValue = (IColumn) null;
      this.count = 0L;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (Utils.IsCharacterDataType(this.expression.DataType))
      {
        this.dataType = VistaDBType.Float;
      }
      else
      {
        if (!Utils.IsNumericDataType(this.expression.DataType))
          throw new VistaDBSQLException(550, "AVG", this.lineNo, this.symbolNo);
        this.dataType = this.expression.DataType;
      }
      this.srcValue = this.CreateColumn(this.expression.DataType);
      this.dstValue = this.CreateColumn(this.dataType);
      this.tmpValue = this.CreateColumn(VistaDBType.BigInt);
      return signatureType;
    }

    protected override void InternalSerialize(ref object serObj)
    {
      AvgFunction.SerializedValue serializedValue;
      if (serObj == null)
      {
        serializedValue = new AvgFunction.SerializedValue();
        serObj = (object) serializedValue;
      }
      else
        serializedValue = (AvgFunction.SerializedValue) serObj;
      serializedValue.Count = this.count;
      serializedValue.Value = this.val;
    }

    protected override void InternalDeserialize(object serObj)
    {
      AvgFunction.SerializedValue serializedValue = (AvgFunction.SerializedValue) serObj;
      this.val = serializedValue.Value;
      this.count = serializedValue.Count;
    }

    protected override object InternalCreateEmptyResult()
    {
      return (object) null;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      if (newVal == null)
      {
        this.count = 0L;
        return (object) null;
      }
      this.count = 1L;
      ((IValue) this.srcValue).Value = newVal;
      this.Convert((IValue) this.srcValue, (IValue) this.dstValue);
      return ((IValue) this.dstValue).Value;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return this.val;
      ((IValue) this.srcValue).Value = newVal;
      this.Convert((IValue) this.srcValue, (IValue) this.dstValue);
      ++this.count;
      if (this.val == null)
        return ((IValue) this.dstValue).Value;
      ((IValue) this.result).Value = this.val;
      return ((Row.Column) this.result + (Row.Column) this.dstValue).Value;
    }

    protected override object InternalFinishGroup()
    {
      if (this.val == null)
        return this.val;
      ((IValue) this.result).Value = this.val;
      ((IValue) this.tmpValue).Value = (object) this.count;
      this.Convert((IValue) ((Row.Column) this.result / (Row.Column) this.tmpValue), (IValue) this.result);
      return ((IValue) this.result).Value;
    }

    private class SerializedValue
    {
      public long Count;
      public object Value;
    }
  }
}
