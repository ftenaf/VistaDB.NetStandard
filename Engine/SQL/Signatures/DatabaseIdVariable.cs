using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DatabaseIdVariable : SystemVariable
  {
    internal DatabaseIdVariable(SQLParser parser)
      : base(parser)
    {
      dataType = VistaDBType.UniqueIdentifier;
    }

    protected override IColumn InternalExecute()
    {
      ((IValue) result).Value = (object) parent.Database.VersionGuid;
      return result;
    }

    public override SignatureType OnPrepare()
    {
      return signatureType;
    }

    protected override bool InternalGetIsChanged()
    {
      return false;
    }
  }
}
