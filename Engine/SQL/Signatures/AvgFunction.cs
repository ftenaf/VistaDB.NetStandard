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
      srcValue = null;
      dstValue = null;
      tmpValue = null;
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
        serObj = serializedValue;
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
      return null;
    }

    protected override object InternalCreateNewGroup(object newVal)
    {
      if (newVal == null)
      {
        count = 0L;
        return null;
      }
      count = 1L;
            srcValue.Value = newVal;
      Convert(srcValue, dstValue);
      return dstValue.Value;
    }

    protected override object InternalAddRowToGroup(object newVal)
    {
      if (newVal == null)
        return val;
            srcValue.Value = newVal;
      Convert(srcValue, dstValue);
      ++count;
      if (val == null)
        return dstValue.Value;
            result.Value = val;
      return ((Row.Column) result + (Row.Column) dstValue).Value;
    }

    protected override object InternalFinishGroup()
    {
      if (val == null)
        return val;
            result.Value = val;
            tmpValue.Value = count;
      Convert((Row.Column)result / (Row.Column)tmpValue, result);
      return result.Value;
    }

    private class SerializedValue
    {
      public long Count;
      public object Value;
    }
  }
}
