using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IdentityVariable : SystemVariable
  {
    public IdentityVariable(SQLParser parser)
      : base(parser)
    {
      dataType = VistaDBType.Unknown;
      parent.AlwaysPrepare(this);
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }

    public override SignatureType OnPrepare()
    {
      IVistaDBColumn lastIdentity = parent.Database.GetLastIdentity(null, null);
      VistaDBType vistaDbType = lastIdentity == null ? VistaDBType.Int : lastIdentity.Type;
      if (vistaDbType != dataType)
      {
        dataType = vistaDbType;
        result = null;
      }
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      IVistaDBColumn lastIdentity = parent.Database.GetLastIdentity(null, null);
            result.Value = lastIdentity == null ? null : lastIdentity.Value;
      return result;
    }
  }
}
