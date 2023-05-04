using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ExistsOperator : UnaryOperator
  {
    public ExistsOperator(SQLParser parser)
      : base(parser, -1)
    {
      if (!(operand is SubQuerySignature))
        throw new VistaDBSQLException(507, "subquery", lineNo, symbolNo);
    }

    protected override IColumn InternalExecute()
    {
      if (GetIsChanged())
        ((IValue) result).Value = (object) ((SubQuerySignature) operand).IsResultPresent();
      return result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      dataType = VistaDBType.Bit;
      if (AlwaysNull)
        return SignatureType.Constant;
      return signatureType;
    }

    public override bool IsNull
    {
      get
      {
        return operand.IsNull;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
