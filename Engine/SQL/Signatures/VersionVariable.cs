using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class VersionVariable : SystemVariable
  {
    private bool changed;

    public VersionVariable(SQLParser parser)
      : base(parser)
    {
      this.dataType = VistaDBType.NChar;
      this.changed = true;
      this.optimizable = true;
    }

    protected override bool InternalGetIsChanged()
    {
      return this.changed;
    }

    public override SignatureType OnPrepare()
    {
      this.changed = true;
      return this.signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (this.changed)
      {
        ((IValue) this.result).Value = (object) Database.DatabaseHeader.FileCopyrightString;
        this.changed = false;
      }
      return this.result;
    }
  }
}
