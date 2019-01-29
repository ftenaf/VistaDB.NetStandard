using System.Collections;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IRow : IVistaDBRow, IEnumerable
  {
    IColumn this[int index] { get; }

    IRow CopyInstance();

    uint RowId { get; set; }
  }
}
