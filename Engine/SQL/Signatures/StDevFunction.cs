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
      srcValue = (IColumn) null;
      dstValue = (IColumn) null;
      squareSum = 0.0;
      sum = 0.0;
      count = 0L;
      dataType = VistaDBType.Float;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (!Utils.CompatibleTypes(dataType, expression.DataType))
        throw new VistaDBSQLException(550, "STDEV", lineNo, symbolNo);
      srcValue = CreateColumn(expression.DataType);
      dstValue = CreateColumn(dataType);
      return signatureType;
    }

    protected override void InternalSerialize(ref object serObj)
    {
            SerializedValue serializedValue;
      if (serObj == null)
      {
        serializedValue = new SerializedValue();
        serObj = (object) serializedValue;
      }
      else
        serializedValue = (SerializedValue) serObj;
      serializedValue.SquareSum = squareSum;
      serializedValue.Sum = sum;
      serializedValue.Count = count;
    }

    protected override void InternalDeserialize(object serObj)
    {
            SerializedValue serializedValue = (SerializedValue) serObj;
      squareSum = serializedValue.SquareSum;
      sum = serializedValue.Sum;
      count = serializedValue.Count;
    }

    protected override object InternalCreateEmptyResult()
    {
      return (object) null;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      if (newVal == null)
      {
        count = 0L;
        sum = 0.0;
        squareSum = 0.0;
        return (object) null;
      }
      count = 1L;
      ((IValue) srcValue).Value = newVal;
      Convert((IValue) srcValue, (IValue) dstValue);
      sum = (double) ((IValue) dstValue).Value;
      squareSum = sum * sum;
      return (object) null;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return (object) null;
      ++count;
      ((IValue) srcValue).Value = newVal;
      Convert((IValue) srcValue, (IValue) dstValue);
      double num = (double) ((IValue) dstValue).Value;
      sum += num;
      squareSum += num * num;
      return (object) null;
    }

    protected override object InternalFinishGroup()
    {
      if (count == 0L)
        return (object) null;
      return (object) Math.Sqrt((squareSum - sum * sum / (double) count) / (double) (count - 1L));
    }

    private class SerializedValue
    {
      public double SquareSum;
      public double Sum;
      public long Count;
    }
  }
}
