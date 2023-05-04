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
        return Iterator < 0;
      }
    }

    internal int ActiveUnits
    {
      get
      {
        return Iterator;
      }
    }

    internal void Push(PCodeUnit unit)
    {
      Add(unit);
      ++Iterator;
    }

    internal PCodeUnit Pop()
    {
      return this[Iterator--];
    }

    internal PCodeUnit PopBack()
    {
      return this[++Iterator];
    }

    internal PCodeUnit PopAndFree()
    {
      try
      {
        return this[Iterator];
      }
      finally
      {
        RemoveAt(Iterator--);
      }
    }

    internal PCodeUnit Peek()
    {
      return this[Iterator];
    }

    internal void ClearCode()
    {
      Clear();
      Iterator = -1;
    }

    internal void ExtractPosition(int position)
    {
      RemoveAt(position);
      if (Iterator < position)
        return;
      --Iterator;
    }

    internal void MovePeekBy(int count)
    {
      Iterator += count;
    }

    internal bool GoHead()
    {
      Iterator = -1;
      return Count > 0;
    }

    internal bool GoTail()
    {
      Iterator = Count - 1;
      return Count > 0;
    }
  }
}
