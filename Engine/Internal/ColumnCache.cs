namespace VistaDB.Engine.Internal
{
  internal class ColumnCache
  {
    public ITableCache TableCache { get; private set; }

    public string ResultColumnName { get; private set; }

    public int ResultColumnIndex { get; private set; }

    public ColumnCache(ITableCache tableCache, string resultColumnName, int resultColumnIndex)
    {
      this.TableCache = tableCache;
      this.ResultColumnName = resultColumnName;
      this.ResultColumnIndex = resultColumnIndex;
    }

    public object GetValue(object key)
    {
      object[] values = this.TableCache.GetValues(key);
      if (values != null && values.Length > this.ResultColumnIndex)
        return values[this.ResultColumnIndex];
      return (object) null;
    }
  }
}
