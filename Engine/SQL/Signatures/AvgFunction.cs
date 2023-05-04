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
      srcValue = (IColumn) null;
      dstValue = (IColumn) null;
      tmpValue = (IColumn) null;
      count = 0L;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (Utils.IsCharacterDataType(expression.DataType))
      {
        dataType = VistaDBType.Float;
      }
      else
      {
        if (!Utils.IsNumericDataType(expression.DataType))
          throw new VistaDBSQLException(550, "AVG", lineNo, symbolNo);
        dataType = expression.DataType;
      }
      srcValue = CreateColumn(expression.DataType);
      dstValue = CreateColumn(dataType);
      tmpValue = CreateColumn(VistaDBType.BigInt);
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
      serializedValue.Count = count;
      serializedValue.Value = val;
    }

    protected override void InternalDeserialize(object serObj)
    {
            SerializedValue serializedValue = (SerializedValue) serObj;
      val = serializedValue.Value;
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
        return (object) null;
      }
      count = 1L;
      ((IValue) srcValue).Value = newVal;
      Convert((IValue) srcValue, (IValue) dstValue);
      return ((IValue) dstValue).Value;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return val;
      ((IValue) srcValue).Value = newVal;
      Convert((IValue) srcValue, (IValue) dstValue);
      ++count;
      if (val == null)
        return ((IValue) dstValue).Value;
      ((IValue) result).Value = val;
      return ((Row.Column) result + (Row.Column) dstValue).Value;
    }

    protected override object InternalFinishGroup()
    {
      if (val == null)
        return val;
      ((IValue) result).Value = val;
      ((IValue) tmpValue).Value = (object) count;
      Convert((IValue) ((Row.Column) result / (Row.Column) tmpValue), (IValue) result);
      return ((IValue) result).Value;
    }

    private class SerializedValue
    {
      public long Count;
      public object Value;
    }
  }
}
