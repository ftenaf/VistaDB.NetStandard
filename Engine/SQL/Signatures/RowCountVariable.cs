using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RowCountVariable : SystemVariable
  {
    internal RowCountVariable(SQLParser parser)
      : base(parser)
    {
      parent.AlwaysPrepare((Signature) this);
    }

    protected override IColumn InternalExecute()
    {
      ((IValue) result).Value = (object) parent.Connection.CachedAffectedRows;
      return result;
    }

    public override SignatureType OnPrepare()
    {
      dataType = VistaDBType.BigInt;
      return SignatureType.Expression;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
