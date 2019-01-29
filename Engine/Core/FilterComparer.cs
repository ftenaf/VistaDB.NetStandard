using System.Collections.Generic;

namespace VistaDB.Engine.Core
{
  internal class FilterComparer : IComparer<Filter>
  {
    public int Compare(Filter x, Filter y)
    {
      int num = x.typeId - y.typeId;
      if (num != 0)
        return num;
      return x.Priority - y.Priority;
    }
  }
}
