using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class IdentityVariable : SystemVariable
  {
    public IdentityVariable(SQLParser parser)
      : base(parser)
    {
      this.dataType = VistaDBType.Unknown;
      this.parent.AlwaysPrepare((Signature) this);
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }

    public override SignatureType OnPrepare()
    {
      IVistaDBColumn lastIdentity = this.parent.Database.GetLastIdentity((string) null, (string) null);
      VistaDBType vistaDbType = lastIdentity == null ? VistaDBType.Int : lastIdentity.Type;
      if (vistaDbType != this.dataType)
      {
        this.dataType = vistaDbType;
        this.result = (IColumn) null;
      }
      return this.signatureType;
    }

    protected override IColumn InternalExecute()
    {
      IVistaDBColumn lastIdentity = this.parent.Database.GetLastIdentity((string) null, (string) null);
      ((IValue) this.result).Value = lastIdentity == null ? (object) null : lastIdentity.Value;
      return this.result;
    }
  }
}
