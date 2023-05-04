using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SumFunction : AggregateFunction
  {
    private IColumn srcValue;
    private IColumn dstValue;

    public SumFunction(SQLParser parser)
      : base(parser, false)
    {
      srcValue = null;
      dstValue = null;
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
          throw new VistaDBSQLException(550, "SUM", lineNo, symbolNo);
        dataType = expression.DataType;
      }
      srcValue = CreateColumn(expression.DataType);
      dstValue = CreateColumn(dataType);
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
      if (val == null)
        return dstValue.Value;
            result.Value = val;
      return ((Row.Column) result + (Row.Column) dstValue).Value;
    }

    protected override object InternalFinishGroup()
    {
      return val;
    }
  }
}
