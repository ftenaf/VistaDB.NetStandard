namespace VistaDB.Engine.Core
{
  internal class SyncTimestamp : Timestamp
  {
    internal SyncTimestamp()
      : base(0L)
    {
    }

    private SyncTimestamp(SyncTimestamp column)
      : base(column)
    {
    }

    protected override Row.Column OnDuplicate(bool padRight)
    {
      return new SyncTimestamp(this);
    }

    internal override bool IsSync
    {
      get
      {
        return true;
      }
    }
  }
}
