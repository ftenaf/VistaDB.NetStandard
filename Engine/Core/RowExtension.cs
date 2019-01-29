using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core
{
  internal class RowExtension : List<IColumn>
  {
    private Row parentRow;

    internal RowExtension(Row originRow)
    {
      this.parentRow = originRow;
    }
  }
}
