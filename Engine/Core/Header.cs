using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class Header : Row
  {
    private uint lastDataVersionOnDisk = Row.MinVersion;
    private int signatureEntry;
    private int rowNumberEntry;
    private ulong position;
    private DataStorage parentStorage;
    private bool modified;
    private bool modifiedVersion;
    private int pageSize;

    protected Header(DataStorage parentStorage, Header.HeaderId id, ulong dataReference, int signature, int pageSize)
      : base((uint) id, 0U, dataReference, true, parentStorage.Encryption, (int[]) null)
    {
      this.parentStorage = parentStorage;
      this.pageSize = pageSize;
      this.alignment = true;
      this.signatureEntry = this.AppendColumn((IColumn) new IntColumn(signature));
      this.rowNumberEntry = this.AppendColumn((IColumn) new IntColumn(0));
    }

    protected DataStorage ParentStorage
    {
      get
      {
        return this.parentStorage;
      }
    }

    internal uint Signature
    {
      get
      {
        return (uint) (int) this[this.signatureEntry].Value;
      }
      set
      {
        this.Modified = (int) this.Signature != (int) value;
        this[this.signatureEntry].Value = (object) (int) value;
      }
    }

    internal uint RowCount
    {
      get
      {
        return (uint) (int) this[this.rowNumberEntry].Value;
      }
      set
      {
        this.Modified = (int) this.RowCount != (int) value;
        this[this.rowNumberEntry].Value = (object) (int) value;
      }
    }

    internal uint Version
    {
      get
      {
        return this.RowVersion;
      }
      set
      {
        this.ModifiedVersion = (int) this.RowVersion != (int) value;
        this.RowVersion = value;
      }
    }

    internal int Size
    {
      get
      {
        return this.Buffer.Length;
      }
    }

    internal int PageSize
    {
      get
      {
        return this.pageSize;
      }
    }

    internal void Activate(ulong position)
    {
      this.position = position;
      this.OnActivate(position);
    }

    internal void Build(ulong position)
    {
      this.position = position;
      this.OnBuild(position);
    }

    internal void ResetVersionInfo()
    {
      this.DoResetVersion();
      this.modified = false;
      this.modifiedVersion = false;
    }

    internal void KeepSchemaVersion()
    {
      this.DoKeepVersion();
    }

    internal void Flush()
    {
      if (!this.modified)
      {
        if (!this.modifiedVersion)
          return;
      }
      try
      {
        this.Write(this.parentStorage, this.modified ? Row.RowScope.All : Row.RowScope.Head);
        this.modified = false;
        this.modifiedVersion = false;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 106, this.parentStorage.Name);
      }
    }

    internal void Update()
    {
      this.OnUpdate();
    }

    protected virtual void OnUpdate()
    {
      this.Read(Row.RowScope.All);
    }

    protected virtual void DoResetVersion()
    {
      this.Version = this.lastDataVersionOnDisk;
    }

    protected virtual void DoKeepVersion()
    {
    }

    internal virtual bool Modified
    {
      get
      {
        if (!this.modifiedVersion)
          return this.modified;
        return true;
      }
      set
      {
        this.modified = value || this.modified;
      }
    }

    protected virtual bool ModifiedVersion
    {
      get
      {
        return this.modifiedVersion;
      }
      set
      {
        this.modifiedVersion = value || this.modifiedVersion;
      }
    }

    internal virtual bool NewVersion
    {
      get
      {
        long version = (long) this.Version;
        this.Read(this.ParentStorage.VirtualLocks ? Row.RowScope.All : Row.RowScope.Head);
        return (long) this.Version != version;
      }
    }

    protected virtual void OnActivate(ulong position)
    {
      this.AssignBuffer();
      this.Position = position;
      this.Read(Row.RowScope.All);
    }

    protected virtual void OnBuild(ulong position)
    {
      this.AssignBuffer();
      this.Position = position;
      this.Flush();
    }

    protected virtual void OnAfterRead(int pageSize, bool justVersion)
    {
      if (pageSize != 0)
      {
        if (pageSize % 1024 != 0)
          throw new VistaDBException(53, "Pagesize must be a 1Kb increment");
        if (pageSize > 16384)
          throw new VistaDBException(53, "Pagesize must be less than 16 Kb");
      }
      this.pageSize = pageSize;
    }

    private void Read(Row.RowScope scope)
    {
      try
      {
        this.Read(this.parentStorage, scope, true);
        this.OnAfterRead(0, scope == Row.RowScope.Head);
        this.lastDataVersionOnDisk = this.Version;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 105, this.parentStorage.Name);
      }
    }

    internal void AssignBuffer()
    {
      int memoryApartment = this.GetMemoryApartment((Row) null);
      int pageSize = this.PageSize;
      this.FormatLength = memoryApartment + (pageSize - memoryApartment % pageSize) % pageSize;
    }

    internal enum HeaderId : uint
    {
      ROW = 1,
      INDEX_NODE = 2,
      ROWSET_HEADER = 3,
      CLUSTERED_ROWSET_HEADER = 4,
      DATABASE_HEADER = 5,
      INDEX_HEADER = 6,
      INDEX_NODE_CRYPT = 7,
      TRANSACTION_LOG = 8,
      NONUSED = 255, // 0x000000FF
    }
  }
}
