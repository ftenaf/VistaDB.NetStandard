using System.Collections;

namespace VistaDB.Extra.Internal
{
  internal class DataTableEnumerator : IEnumerator
  {
    private VistaDBDataTable parent;
    private int rowPos;

    public DataTableEnumerator(VistaDBDataTable parent)
    {
      this.parent = parent;
      Reset();
    }

    public object Current
    {
      get
      {
        return null;
      }
    }

    public bool MoveNext()
    {
      if (rowPos >= parent.Count - 2)
        return false;
      ++rowPos;
      return true;
    }

    public void Reset()
    {
    }
  }
}
