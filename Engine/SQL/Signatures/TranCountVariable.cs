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
                Result.Value = -1;
      else
                result.Value = parent.Connection.Database.NestedTransactionLevel;
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
