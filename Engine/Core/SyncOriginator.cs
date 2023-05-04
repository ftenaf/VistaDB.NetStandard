using System;

namespace VistaDB.Engine.Core
{
  internal class SyncOriginator : UniqueIdentifierColumn
  {
    internal SyncOriginator()
      : base(Guid.Empty)
    {
    }

    private SyncOriginator(SyncOriginator column)
      : base(column)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new SyncOriginator(this);
    }

    internal override bool IsSync
    {
      get
      {
        return true;
      }
    }

    internal override void AssignAttributes(string name, bool allowNull, bool readOnly, bool encrypted, bool packed, string caption, string description)
    {
      allowNull = false;
      base.AssignAttributes(name, allowNull, readOnly, encrypted, packed, caption, description);
    }
  }
}
