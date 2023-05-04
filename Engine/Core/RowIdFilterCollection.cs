using System.Collections.Generic;

namespace VistaDB.Engine.Core
{
  internal class RowIdFilterCollection : Dictionary<uint, RowIdFilterCollection.FilterItem>
  {
    internal RowIdFilter GetFilter(Row lowConstant, Row highConstant, bool excludeNulls)
    {
      if (!ContainsKey(lowConstant.RowId))
        return (RowIdFilter) null;
            FilterItem filterItem = this[lowConstant.RowId];
      if (!excludeNulls || !filterItem.Nulls || highConstant - filterItem.High != 0)
        return (RowIdFilter) null;
      return filterItem.Filter.Clone();
    }

    internal void PutFilter(RowIdFilter filter, Row lowConstant, Row highConstant, bool excludeNulls)
    {
      if (ContainsKey(lowConstant.RowId))
        return;
            FilterItem filterItem = new FilterItem(filter.Clone(), highConstant, excludeNulls);
      Add(lowConstant.RowId, filterItem);
    }

    internal struct FilterItem
    {
      private RowIdFilter filter;
      private Row high;
      private bool nulls;

      internal FilterItem(RowIdFilter filter, Row high, bool nulls)
      {
        this.filter = filter;
        this.high = high;
        this.nulls = nulls;
      }

      internal RowIdFilter Filter
      {
        get
        {
          return filter;
        }
      }

      internal Row High
      {
        get
        {
          return high;
        }
      }

      internal bool Nulls
      {
        get
        {
          return nulls;
        }
      }
    }
  }
}
