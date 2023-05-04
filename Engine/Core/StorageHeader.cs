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

    protected StorageHeader(DataStorage parentStorage, HeaderId id, ulong dataReference, int signature, int pageSize, CultureInfo culture)
      : base(parentStorage, id, dataReference, signature, pageSize)
    {
      pageSizeEntry = AppendColumn((IColumn) new SmallIntColumn((short) (pageSize / StorageHandle.DEFAULT_SIZE_OF_PAGE)));
      localeEntry = AppendColumn((IColumn) new IntColumn(culture.LCID));
      defaultCulture = culture;
      idCounterEntry = AppendColumn((IColumn) new IntColumn(0));
      timestampEntry = AppendColumn((IColumn) new BigIntColumn(0L));
      AppendColumn((IColumn) new BitColumn(false));
      schemaVersion = AppendColumn((IColumn) new IntColumn(0));
      transactionLogTableId = AppendColumn((IColumn) new BigIntColumn(0L));
    }

    internal CultureInfo Culture
    {
      get
      {
        return defaultCulture;
      }
    }

    internal uint AutoId
    {
      get
      {
        uint num1 = (uint) (int) this[idCounterEntry].Value;
        uint num2;
        this[idCounterEntry].Value = (object) (int) (num2 = num1 + 1U);
        Modified = true;
        return num2;
      }
    }

    internal uint CurrentAutoId
    {
      get
      {
        return (uint) (int) this[idCounterEntry].Value;
      }
    }

    internal ulong CurrentTimestampId
    {
      get
      {
        return (ulong) (long) this[timestampEntry].Value;
      }
    }

    internal ulong NextTimestampId
    {
      get
      {
        ulong currentTimestampId = CurrentTimestampId;
        ulong num;
        this[timestampEntry].Value = (object) (long) (num = currentTimestampId + 1UL);
        Modified = true;
        return num;
      }
    }

    internal void InitTimestamp(ulong timestamp)
    {
      this[timestampEntry].Value = (object) (long) timestamp;
    }

    internal new ulong RefPosition
    {
      get
      {
        return base.RefPosition;
      }
      set
      {
        Modified = (long) base.RefPosition != (long) value;
        base.RefPosition = value;
      }
    }

    internal int SchemaVersion
    {
      get
      {
        return (int) this[schemaVersion].Value;
      }
      set
      {
        Modified = SchemaVersion != value;
        this[schemaVersion].Value = (object) value;
      }
    }

    internal ulong TransactionLogPosition
    {
      get
      {
        return (ulong) (long) this[transactionLogTableId].Value;
      }
      set
      {
        Modified = (long) TransactionLogPosition != (long) value;
        this[transactionLogTableId].Value = (object) (long) value;
      }
    }

    protected override void OnUpdate()
    {
      long schemaVersion = (long) SchemaVersion;
      base.OnUpdate();
      if (schemaVersion != (long) SchemaVersion)
        throw new VistaDBException(140, ParentStorage.Name);
    }

    protected override void OnAfterRead(int pageSize, bool justVersion)
    {
      if (!justVersion)
        base.OnAfterRead((int) (short) this[pageSizeEntry].Value * StorageHandle.DEFAULT_SIZE_OF_PAGE, justVersion);
      lastDataSchemaVersionOnDisk = SchemaVersion;
    }

    protected override void DoResetVersion()
    {
      base.DoResetVersion();
      SchemaVersion = lastDataSchemaVersionOnDisk;
    }

    protected override void DoKeepVersion()
    {
      base.DoKeepVersion();
      lastDataSchemaVersionOnDisk = SchemaVersion;
    }

    protected void ReinitializeCulture()
    {
      int culture = (int) this[localeEntry].Value;
      if (culture == 0)
        throw new VistaDBException(104);
      defaultCulture = new CultureInfo(culture);
    }
  }
}
