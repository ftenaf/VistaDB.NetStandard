using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RowCountVariable : SystemVariable
  {
    internal RowCountVariable(SQLParser parser)
      : base(parser)
    {
      this.parent.AlwaysPrepare((Signature) this);
    }

    protected override IColumn InternalExecute()
    {
      ((IValue) this.result).Value = (object) this.parent.Connection.CachedAffectedRows;
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      this.dataType = VistaDBType.BigInt;
      return SignatureType.Expression;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
