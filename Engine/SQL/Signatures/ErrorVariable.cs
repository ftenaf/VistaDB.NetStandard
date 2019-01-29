using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ErrorVariable : SystemVariable
  {
    internal ErrorVariable(SQLParser parser)
      : base(parser)
    {
      this.dataType = VistaDBType.Int;
    }

    protected override IColumn InternalExecute()
    {
      VistaDBException lastException = this.parent.Connection.LastException;
      ((IValue) this.result).Value = (object) (lastException == null ? 0 : lastException.ErrorId);
      return this.result;
    }

    public override SignatureType OnPrepare()
    {
      return this.signatureType;
    }

    protected override bool InternalGetIsChanged()
    {
      return false;
    }
  }
}
