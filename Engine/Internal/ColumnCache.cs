namespace VistaDB.Engine.Internal
{
  internal class ColumnCache
  {
    public ITableCache TableCache { get; private set; }

    public string ResultColumnName { get; private set; }

    public int ResultColumnIndex { get; private set; }

    public ColumnCache(ITableCache tableCache, string resultColumnName, int resultColumnIndex)
    {
      TableCache = tableCache;
      ResultColumnName = resultColumnName;
      ResultColumnIndex = resultColumnIndex;
    }

    public object GetValue(object key)
    {
      object[] values = TableCache.GetValues(key);
      if (values != null && values.Length > ResultColumnIndex)
        return values[ResultColumnIndex];
      return (object) null;
    }
  }
}
