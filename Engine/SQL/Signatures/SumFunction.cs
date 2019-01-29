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
      this.srcValue = (IColumn) null;
      this.dstValue = (IColumn) null;
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
          throw new VistaDBSQLException(550, "SUM", this.lineNo, this.symbolNo);
        this.dataType = this.expression.DataType;
      }
      this.srcValue = this.CreateColumn(this.expression.DataType);
      this.dstValue = this.CreateColumn(this.dataType);
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
      if (this.val == null)
        return ((IValue) this.dstValue).Value;
      ((IValue) this.result).Value = this.val;
      return ((Row.Column) this.result + (Row.Column) this.dstValue).Value;
    }

    protected override object InternalFinishGroup()
    {
      return this.val;
    }
  }
}
