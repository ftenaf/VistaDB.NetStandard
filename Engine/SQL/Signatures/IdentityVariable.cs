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
      parent.AlwaysPrepare((Signature) this);
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }

    public override SignatureType OnPrepare()
    {
      IVistaDBColumn lastIdentity = parent.Database.GetLastIdentity((string) null, (string) null);
      VistaDBType vistaDbType = lastIdentity == null ? VistaDBType.Int : lastIdentity.Type;
      if (vistaDbType != dataType)
      {
        dataType = vistaDbType;
        result = (IColumn) null;
      }
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      IVistaDBColumn lastIdentity = parent.Database.GetLastIdentity((string) null, (string) null);
      ((IValue) result).Value = lastIdentity == null ? (object) null : lastIdentity.Value;
      return result;
    }
  }
}
