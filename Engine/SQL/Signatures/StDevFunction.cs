using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StDevFunction : AggregateFunction
  {
    private IColumn srcValue;
    private IColumn dstValue;
    private double squareSum;
    private double sum;
    private long count;

    public StDevFunction(SQLParser parser)
      : base(parser, false)
    {
      this.srcValue = (IColumn) null;
      this.dstValue = (IColumn) null;
      this.squareSum = 0.0;
      this.sum = 0.0;
      this.count = 0L;
      this.dataType = VistaDBType.Float;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (!Utils.CompatibleTypes(this.dataType, this.expression.DataType))
        throw new VistaDBSQLException(550, "STDEV", this.lineNo, this.symbolNo);
      this.srcValue = this.CreateColumn(this.expression.DataType);
      this.dstValue = this.CreateColumn(this.dataType);
      return signatureType;
    }

    protected override void InternalSerialize(ref object serObj)
    {
      StDevFunction.SerializedValue serializedValue;
      if (serObj == null)
      {
        serializedValue = new StDevFunction.SerializedValue();
        serObj = (object) serializedValue;
      }
      else
        serializedValue = (StDevFunction.SerializedValue) serObj;
      serializedValue.SquareSum = this.squareSum;
      serializedValue.Sum = this.sum;
      serializedValue.Count = this.count;
    }

    protected override void InternalDeserialize(object serObj)
    {
      StDevFunction.SerializedValue serializedValue = (StDevFunction.SerializedValue) serObj;
      this.squareSum = serializedValue.SquareSum;
      this.sum = serializedValue.Sum;
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
        this.sum = 0.0;
        this.squareSum = 0.0;
        return (object) null;
      }
      this.count = 1L;
      ((IValue) this.srcValue).Value = newVal;
      this.Convert((IValue) this.srcValue, (IValue) this.dstValue);
      this.sum = (double) ((IValue) this.dstValue).Value;
      this.squareSum = this.sum * this.sum;
      return (object) null;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return (object) null;
      ++this.count;
      ((IValue) this.srcValue).Value = newVal;
      this.Convert((IValue) this.srcValue, (IValue) this.dstValue);
      double num = (double) ((IValue) this.dstValue).Value;
      this.sum += num;
      this.squareSum += num * num;
      return (object) null;
    }

    protected override object InternalFinishGroup()
    {
      if (this.count == 0L)
        return (object) null;
      return (object) Math.Sqrt((this.squareSum - this.sum * this.sum / (double) this.count) / (double) (this.count - 1L));
    }

    private class SerializedValue
    {
      public double SquareSum;
      public double Sum;
      public long Count;
    }
  }
}
