using System.Collections.Generic;

namespace VistaDB.Engine.Core
{
  internal class RowIdFilterCollection : Dictionary<uint, RowIdFilterCollection.FilterItem>
  {
    internal RowIdFilter GetFilter(Row lowConstant, Row highConstant, bool excludeNulls)
    {
      if (!this.ContainsKey(lowConstant.RowId))
        return (RowIdFilter) null;
      RowIdFilterCollection.FilterItem filterItem = this[lowConstant.RowId];
      if (!excludeNulls || !filterItem.Nulls || highConstant - filterItem.High != 0)
        return (RowIdFilter) null;
      return filterItem.Filter.Clone();
    }

    internal void PutFilter(RowIdFilter filter, Row lowConstant, Row highConstant, bool excludeNulls)
    {
      if (this.ContainsKey(lowConstant.RowId))
        return;
      RowIdFilterCollection.FilterItem filterItem = new RowIdFilterCollection.FilterItem(filter.Clone(), highConstant, excludeNulls);
      this.Add(lowConstant.RowId, filterItem);
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
          return this.filter;
        }
      }

      internal Row High
      {
        get
        {
          return this.high;
        }
      }

      internal bool Nulls
      {
        get
        {
          return this.nulls;
        }
      }
    }
  }
}
