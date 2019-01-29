using System;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NewIDFunction : Function
  {
    public NewIDFunction(SQLParser parser)
      : base(parser, 0, true)
    {
      this.dataType = VistaDBType.UniqueIdentifier;
    }

    public override SignatureType OnPrepare()
    {
      return SignatureType.Expression;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Guid.NewGuid();
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
