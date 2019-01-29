using System.Collections.Generic;

namespace VistaDB.Engine.Core.Scripting
{
  internal class ProcedureCode : List<PCodeUnit>
  {
    internal int Iterator = -1;

    internal ProcedureCode()
    {
    }

    internal bool Empty
    {
      get
      {
        return this.Iterator < 0;
      }
    }

    internal int ActiveUnits
    {
      get
      {
        return this.Iterator;
      }
    }

    internal void Push(PCodeUnit unit)
    {
      this.Add(unit);
      ++this.Iterator;
    }

    internal PCodeUnit Pop()
    {
      return this[this.Iterator--];
    }

    internal PCodeUnit PopBack()
    {
      return this[++this.Iterator];
    }

    internal PCodeUnit PopAndFree()
    {
      try
      {
        return this[this.Iterator];
      }
      finally
      {
        this.RemoveAt(this.Iterator--);
      }
    }

    internal PCodeUnit Peek()
    {
      return this[this.Iterator];
    }

    internal void ClearCode()
    {
      this.Clear();
      this.Iterator = -1;
    }

    internal void ExtractPosition(int position)
    {
      this.RemoveAt(position);
      if (this.Iterator < position)
        return;
      --this.Iterator;
    }

    internal void MovePeekBy(int count)
    {
      this.Iterator += count;
    }

    internal bool GoHead()
    {
      this.Iterator = -1;
      return this.Count > 0;
    }

    internal bool GoTail()
    {
      this.Iterator = this.Count - 1;
      return this.Count > 0;
    }
  }
}
