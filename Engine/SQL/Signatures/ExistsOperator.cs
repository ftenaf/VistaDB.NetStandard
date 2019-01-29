using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ExistsOperator : UnaryOperator
  {
    public ExistsOperator(SQLParser parser)
      : base(parser, -1)
    {
      if (!(this.operand is SubQuerySignature))
        throw new VistaDBSQLException(507, "subquery", this.lineNo, this.symbolNo);
    }

    protected override IColumn InternalExecute()
    {
      if (this.GetIsChanged())
        ((IValue) this.result).Value = (object) ((SubQuerySignature) this.operand).IsResultPresent();
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      this.dataType = VistaDBType.Bit;
      if (this.AlwaysNull)
        return SignatureType.Constant;
      return signatureType;
    }

    public override bool IsNull
    {
      get
      {
        return this.operand.IsNull;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
