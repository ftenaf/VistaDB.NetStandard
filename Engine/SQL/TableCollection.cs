using System.Collections.Generic;

namespace VistaDB.Engine.SQL
{
  internal class TableCollection : List<SourceTable>
  {
    internal void Open()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.Open();
      AllOpen = true;
    }

    internal void Close()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.Close();
      AllOpen = false;
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
      if (HasNative && !sourceTable.IsNativeTable)
        HasNative = false;
      else if (Count == 0 && sourceTable.IsNativeTable)
        HasNative = true;
      Add(sourceTable);
    }

    internal void Free()
    {
      foreach (SourceTable sourceTable in (List<SourceTable>) this)
        sourceTable.FreeTable();
      AllOpen = false;
    }

    internal bool AllOpen { get; private set; }

    internal bool HasNative { get; private set; }
  }
}
