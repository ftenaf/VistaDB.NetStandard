using System.Collections;

namespace VistaDB.DDA
{
  public interface IVistaDBRow : IEnumerable
  {
    int Count { get; }

    IVistaDBColumn this[int index] { get; }

    IVistaDBColumn this[string name] { get; }

    long RowId { get; }

    void ClearModified();

    int Compare(IVistaDBRow row);

    int CompareKey(IVistaDBRow key);

    void InitTop();

    void InitBottom();
  }
}
