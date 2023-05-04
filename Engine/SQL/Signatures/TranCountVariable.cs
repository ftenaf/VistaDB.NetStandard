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
      if (parent.Connection.Database == null)
        ((IValue) Result).Value = (object) -1;
      else
        ((IValue) result).Value = (object) parent.Connection.Database.NestedTransactionLevel;
      parent.Connection.CachedAffectedRows = 1L;
      return result;
    }

    public override SignatureType OnPrepare()
    {
      dataType = VistaDBType.Int;
      return signatureType;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
