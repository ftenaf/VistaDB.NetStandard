namespace VistaDB.Engine.Internal
{
  internal interface IQueryResult
  {
    void FirstRow();

    void NextRow();

    void Close();

    object GetValue(int index, VistaDBType dataType);

    IColumn GetColumn(int index);

    bool IsNull(int index);

    int GetColumnCount();

    bool EndOfTable { get; }

    long RowCount { get; }
  }
}
