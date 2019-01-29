using VistaDB.Engine.Core;

namespace VistaDB
{
  public sealed class TriggerContext
  {
    private Table[] modificationTables;
    private int columnCount;
    private TriggerAction action;
    private byte[] ordinalsCollection;

    internal TriggerContext(Table[] modificationTables, TriggerAction action, int columnCount)
    {
      this.modificationTables = modificationTables;
      this.action = action;
      this.columnCount = columnCount;
      this.ordinalsCollection = new byte[columnCount];
    }

    internal Table[] ModificationTables
    {
      get
      {
        return this.modificationTables;
      }
    }

    public int ColumnCount
    {
      get
      {
        return this.columnCount;
      }
    }

    public TriggerAction TriggerAction
    {
      get
      {
        return this.action;
      }
    }

    public bool IsUpdatedColumn(int columnOrdinal)
    {
      if (this.ordinalsCollection != null)
        return this.ordinalsCollection[columnOrdinal] != (byte) 0;
      return false;
    }

    internal void SetUpdatedColumn(int columnOrdinal)
    {
      if (this.ordinalsCollection == null)
        return;
      this.ordinalsCollection[columnOrdinal] = (byte) 1;
    }
  }
}
