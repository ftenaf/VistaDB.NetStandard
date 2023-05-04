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
      dataType = VistaDBType.NChar;
      changed = true;
      optimizable = true;
    }

    protected override bool InternalGetIsChanged()
    {
      return changed;
    }

    public override SignatureType OnPrepare()
    {
      changed = true;
      return signatureType;
    }

    protected override IColumn InternalExecute()
    {
      if (changed)
      {
                result.Value = Database.DatabaseHeader.FileCopyrightString;
        changed = false;
      }
      return result;
    }
  }
}
