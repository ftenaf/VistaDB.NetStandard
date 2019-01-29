using System.Collections.Generic;

namespace VistaDB.Engine.SQL
{
  internal class TableCollection : List<SourceTable>
  {
    internal void Open()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.Open();
      this.AllOpen = true;
    }

    internal void Close()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.Close();
      this.AllOpen = false;
    }

    internal void Prepare()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.Prepare();
    }

    internal void Unprepare()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.Unprepare();
    }

    internal void AddTable(SourceTable sourceTable)
    {
      if (this.HasNative && !sourceTable.IsNativeTable)
        this.HasNative = false;
      else if (this.Count == 0 && sourceTable.IsNativeTable)
        this.HasNative = true;
      this.Add(sourceTable);
    }

    internal void Free()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.FreeTable();
      this.AllOpen = false;
    }

    internal bool AllOpen { get; private set; }

    internal bool HasNative { get; private set; }
  }
}
