using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class TranCountVariable : SystemVariable
  {
    internal TranCountVariable(SQLParser parser)
      : base(parser)
    {
    }

    protected override IColumn InternalExecute()
    {
      if (this.parent.Connection.Database == null)
        ((IValue) this.Result).Value = (object) -1;
      else
        ((IValue) this.result).Value = (object) this.parent.Connection.Database.NestedTransactionLevel;
      this.parent.Connection.CachedAffectedRows = 1L;
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      this.dataType = VistaDBType.Int;
      return this.signatureType;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
