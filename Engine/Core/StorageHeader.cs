using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core.IO;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class StorageHeader : Header
  {
    private int pageSizeEntry;
    private int localeEntry;
    private int idCounterEntry;
    private int timestampEntry;
    private CultureInfo defaultCulture;
    private int schemaVersion;
    private int lastDataSchemaVersionOnDisk;
    private int transactionLogTableId;

    protected StorageHeader(DataStorage parentStorage, Header.HeaderId id, ulong dataReference, int signature, int pageSize, CultureInfo culture)
      : base(parentStorage, id, dataReference, signature, pageSize)
    {
      this.pageSizeEntry = this.AppendColumn((IColumn) new SmallIntColumn((short) (pageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE)));
      this.localeEntry = this.AppendColumn((IColumn) new IntColumn(culture.LCID));
      this.defaultCulture = culture;
      this.idCounterEntry = this.AppendColumn((IColumn) new IntColumn(0));
      this.timestampEntry = this.AppendColumn((IColumn) new BigIntColumn(0L));
      this.AppendColumn((IColumn) new BitColumn(false));
      this.schemaVersion = this.AppendColumn((IColumn) new IntColumn(0));
      this.transactionLogTableId = this.AppendColumn((IColumn) new BigIntColumn(0L));
    }

    internal CultureInfo Culture
    {
      get
      {
        return this.defaultCulture;
      }
    }

    internal uint AutoId
    {
      get
      {
        uint num1 = (uint) (int) this[this.idCounterEntry].Value;
        uint num2;
        this[this.idCounterEntry].Value = (object) (int) (num2 = num1 + 1U);
        this.Modified = true;
        return num2;
      }
    }

    internal uint CurrentAutoId
    {
      get
      {
        return (uint) (int) this[this.idCounterEntry].Value;
      }
    }

    internal ulong CurrentTimestampId
    {
      get
      {
        return (ulong) (long) this[this.timestampEntry].Value;
      }
    }

    internal ulong NextTimestampId
    {
      get
      {
        ulong currentTimestampId = this.CurrentTimestampId;
        ulong num;
        this[this.timestampEntry].Value = (object) (long) (num = currentTimestampId + 1UL);
        this.Modified = true;
        return num;
      }
    }

    internal void InitTimestamp(ulong timestamp)
    {
      this[this.timestampEntry].Value = (object) (long) timestamp;
    }

    internal new ulong RefPosition
    {
      get
      {
        return base.RefPosition;
      }
      set
      {
        this.Modified = (long) base.RefPosition != (long) value;
        base.RefPosition = value;
      }
    }

    internal int SchemaVersion
    {
      get
      {
        return (int) this[this.schemaVersion].Value;
      }
      set
      {
        this.Modified = this.SchemaVersion != value;
        this[this.schemaVersion].Value = (object) value;
      }
    }

    internal ulong TransactionLogPosition
    {
      get
      {
        return (ulong) (long) this[this.transactionLogTableId].Value;
      }
      set
      {
        this.Modified = (long) this.TransactionLogPosition != (long) value;
        this[this.transactionLogTableId].Value = (object) (long) value;
      }
    }

    protected override void OnUpdate()
    {
      long schemaVersion = (long) this.SchemaVersion;
      base.OnUpdate();
      if (schemaVersion != (long) this.SchemaVersion)
        throw new VistaDBException(140, this.ParentStorage.Name);
    }

    protected override void OnAfterRead(int pageSize, bool justVersion)
    {
      if (!justVersion)
        base.OnAfterRead((int) (short) this[this.pageSizeEntry].Value * StorageHandle.DEFAULT_SIZE_OF_PAGE, justVersion);
      this.lastDataSchemaVersionOnDisk = this.SchemaVersion;
    }

    protected override void DoResetVersion()
    {
      base.DoResetVersion();
      this.SchemaVersion = this.lastDataSchemaVersionOnDisk;
    }

    protected override void DoKeepVersion()
    {
      base.DoKeepVersion();
      this.lastDataSchemaVersionOnDisk = this.SchemaVersion;
    }

    protected void ReinitializeCulture()
    {
      int culture = (int) this[this.localeEntry].Value;
      if (culture == 0)
        throw new VistaDBException(104);
      this.defaultCulture = new CultureInfo(culture);
    }
  }
}
