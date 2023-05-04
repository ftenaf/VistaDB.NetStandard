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
      ordinalsCollection = new byte[columnCount];
    }

    internal Table[] ModificationTables
    {
      get
      {
        return modificationTables;
      }
    }

    public int ColumnCount
    {
      get
      {
        return columnCount;
      }
    }

    public TriggerAction TriggerAction
    {
      get
      {
        return action;
      }
    }

    public bool IsUpdatedColumn(int columnOrdinal)
    {
      if (ordinalsCollection != null)
        return ordinalsCollection[columnOrdinal] != 0;
      return false;
    }

    internal void SetUpdatedColumn(int columnOrdinal)
    {
      if (ordinalsCollection == null)
        return;
      ordinalsCollection[columnOrdinal] = 1;
    }
  }
}
