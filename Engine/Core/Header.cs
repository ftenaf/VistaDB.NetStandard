using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class Header : Row
  {
    private uint lastDataVersionOnDisk = MinVersion;
    private int signatureEntry;
    private int rowNumberEntry;
    private ulong position;
    private DataStorage parentStorage;
    private bool modified;
    private bool modifiedVersion;
    private int pageSize;

    protected Header(DataStorage parentStorage, HeaderId id, ulong dataReference, int signature, int pageSize)
      : base((uint) id, 0U, dataReference, true, parentStorage.Encryption, (int[]) null)
    {
      this.parentStorage = parentStorage;
      this.pageSize = pageSize;
      alignment = true;
      signatureEntry = AppendColumn((IColumn) new IntColumn(signature));
      rowNumberEntry = AppendColumn((IColumn) new IntColumn(0));
    }

    protected DataStorage ParentStorage
    {
      get
      {
        return parentStorage;
      }
    }

    internal uint Signature
    {
      get
      {
        return (uint) (int) this[signatureEntry].Value;
      }
      set
      {
        Modified = (int) Signature != (int) value;
        this[signatureEntry].Value = (object) (int) value;
      }
    }

    internal uint RowCount
    {
      get
      {
        return (uint) (int) this[rowNumberEntry].Value;
      }
      set
      {
        Modified = (int) RowCount != (int) value;
        this[rowNumberEntry].Value = (object) (int) value;
      }
    }

    internal uint Version
    {
      get
      {
        return RowVersion;
      }
      set
      {
        ModifiedVersion = (int) RowVersion != (int) value;
        RowVersion = value;
      }
    }

    internal int Size
    {
      get
      {
        return Buffer.Length;
      }
    }

    internal int PageSize
    {
      get
      {
        return pageSize;
      }
    }

    internal void Activate(ulong position)
    {
      this.position = position;
      OnActivate(position);
    }

    internal void Build(ulong position)
    {
      this.position = position;
      OnBuild(position);
    }

    internal void ResetVersionInfo()
    {
      DoResetVersion();
      modified = false;
      modifiedVersion = false;
    }

    internal void KeepSchemaVersion()
    {
      DoKeepVersion();
    }

    internal void Flush()
    {
      if (!modified)
      {
        if (!modifiedVersion)
          return;
      }
      try
      {
        Write(parentStorage, modified ? RowScope.All : RowScope.Head);
        modified = false;
        modifiedVersion = false;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 106, parentStorage.Name);
      }
    }

    internal void Update()
    {
      OnUpdate();
    }

    protected virtual void OnUpdate()
    {
      Read(RowScope.All);
    }

    protected virtual void DoResetVersion()
    {
      Version = lastDataVersionOnDisk;
    }

    protected virtual void DoKeepVersion()
    {
    }

    internal virtual bool Modified
    {
      get
      {
        if (!modifiedVersion)
          return modified;
        return true;
      }
      set
      {
        modified = value || modified;
      }
    }

    protected virtual bool ModifiedVersion
    {
      get
      {
        return modifiedVersion;
      }
      set
      {
        modifiedVersion = value || modifiedVersion;
      }
    }

    internal virtual bool NewVersion
    {
      get
      {
        long version = (long) Version;
        Read(ParentStorage.VirtualLocks ? RowScope.All : RowScope.Head);
        return (long) Version != version;
      }
    }

    protected virtual void OnActivate(ulong position)
    {
      AssignBuffer();
      Position = position;
      Read(RowScope.All);
    }

    protected virtual void OnBuild(ulong position)
    {
      AssignBuffer();
      Position = position;
      Flush();
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

    private void Read(RowScope scope)
    {
      try
      {
        Read(parentStorage, scope, true);
        OnAfterRead(0, scope == RowScope.Head);
        lastDataVersionOnDisk = Version;
      }
      catch (Exception ex)
      {
        throw new VistaDBException(ex, 105, parentStorage.Name);
      }
    }

    internal void AssignBuffer()
    {
      int memoryApartment = GetMemoryApartment((Row) null);
      int pageSize = PageSize;
      FormatLength = memoryApartment + (pageSize - memoryApartment % pageSize) % pageSize;
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
