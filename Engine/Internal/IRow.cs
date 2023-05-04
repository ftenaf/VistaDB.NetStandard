using System.Collections;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
    internal interface IRow : IVistaDBRow, IEnumerable
    {
        new IColumn this[int index] { get; }

        IRow CopyInstance();

        new uint RowId { get; set; }
    }
}
