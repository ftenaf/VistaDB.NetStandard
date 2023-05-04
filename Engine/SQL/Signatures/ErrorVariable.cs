using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ErrorVariable : SystemVariable
  {
    internal ErrorVariable(SQLParser parser)
      : base(parser)
    {
      dataType = VistaDBType.Int;
    }

    protected override IColumn InternalExecute()
    {
      VistaDBException lastException = parent.Connection.LastException;
            result.Value = lastException == null ? 0 : lastException.ErrorId;
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
