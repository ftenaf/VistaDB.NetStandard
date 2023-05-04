using System.Collections.Generic;

namespace VistaDB.Engine.Core
{
  internal class FiltersList : List<Filter>
  {
    public FiltersList()
    {
    }

    public FiltersList(IEnumerable<Filter> collection)
      : base(collection)
    {
    }

    internal ulong FilteredCount
    {
      get
      {
        if (Count != 0)
          return this[Count - 1].FilteredCount;
        return 0;
      }
    }

    internal void ClearId(Filter.FilterType typeId)
    {
      List<Filter> filterList = new List<Filter>();
      for (int index = 0; index < Count; ++index)
      {
        if (this[index].TypeId == typeId)
          filterList.Add(this[index]);
      }
      for (int index = 0; index < filterList.Count; ++index)
        Remove(filterList[index]);
    }

    internal int AddFilter(Filter filter)
    {
      if (filter == null)
        return -1;
      Add(filter);
      return Count;
    }

    internal void RemoveFilter(Filter filter)
    {
      Remove(filter);
    }

    internal void Activate()
    {
      for (int index = 0; index < Count; ++index)
        this[index].Activate(false);
    }

    internal void Deactivate()
    {
      for (int index = 0; index < Count; ++index)
        this[index].Deactivate();
    }

    internal void ActivateByType(Filter.FilterType typeId)
    {
      for (int index = 0; index < Count; ++index)
      {
        Filter filter = this[index];
        if (filter.TypeId == typeId)
          filter.Activate(false);
      }
    }

    internal void DeactivateByType(Filter.FilterType typeId)
    {
      for (int index = 0; index < Count; ++index)
      {
        Filter filter = this[index];
        if (filter.TypeId == typeId)
          filter.Deactivate();
      }
    }

    internal int CountId(Filter.FilterType typeId)
    {
      int num = 0;
      for (int index = 0; index < Count; ++index)
      {
        if (this[index].TypeId == typeId)
          ++num;
      }
      return num;
    }

    internal bool IsActive(Filter.FilterType typeId)
    {
      for (int index = 0; index < Count; ++index)
      {
        Filter filter = this[index];
        if (filter.TypeId == typeId && filter.Active)
          return true;
      }
      return false;
    }

    internal enum FilterLevel
    {
      NoFilters,
      UsualFilter,
      FullOptimization,
      ForIndexCondition,
    }
  }
}
