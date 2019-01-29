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
      this.Reset();
    }

    public object Current
    {
      get
      {
        return (object) null;
      }
    }

    public bool MoveNext()
    {
      if (this.rowPos >= this.parent.Count - 2)
        return false;
      ++this.rowPos;
      return true;
    }

    public void Reset()
    {
    }
  }
}
