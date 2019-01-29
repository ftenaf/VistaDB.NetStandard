using System;
using System.Collections.Generic;
using VistaDB.DDA;

namespace VistaDB.Extra.Internal
{
  internal class VistaDBRowHashTable : Dictionary<IVistaDBRow, IVistaDBRow>
  {
    internal new IVistaDBRow this[IVistaDBRow key]
    {
      get
      {
        IVistaDBRow vistaDbRow = base[key];
        if (vistaDbRow == null)
          throw new Exception("HashTable for InternalRow cache is null");
        return vistaDbRow;
      }
      set
      {
        base[key] = value;
      }
    }
  }
}
