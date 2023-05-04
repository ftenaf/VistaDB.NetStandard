using System;
using System.Collections.Generic;
using System.Threading;

namespace VistaDB.Engine.Internal
{
  internal class Context
  {
    private Dictionary<int, ContextStack> threadsContext = new Dictionary<int, ContextStack>();
    private bool active = true;

    private static int ThreadId
    {
      get
      {
        return Thread.CurrentThread.ManagedThreadId;
      }
    }

    private ContextStack GetStack()
    {
      if (threadsContext.ContainsKey(ThreadId))
        return threadsContext[ThreadId];
            ContextStack contextStack = new ContextStack();
      threadsContext.Add(ThreadId, contextStack);
      return contextStack;
    }

    private void ReleaseStack()
    {
      threadsContext.Remove(ThreadId);
    }

    internal bool Available
    {
      get
      {
        if (active)
          return CurrentContext != null;
        return false;
      }
    }

    internal IDisposable CurrentContext
    {
      get
      {
        lock (threadsContext)
        {
          if (!active)
            return (IDisposable) null;
          return GetStack().Current;
        }
      }
      set
      {
        lock (threadsContext)
          GetStack().Current = value;
      }
    }

    internal void PushContext(IDisposable newContext)
    {
      lock (threadsContext)
        GetStack().Push(newContext);
    }

    internal void PopContext()
    {
      lock (threadsContext)
      {
                ContextStack stack = GetStack();
        try
        {
          stack.Pop()?.Dispose();
        }
        finally
        {
          if (stack.Count == 0)
            ReleaseStack();
        }
      }
    }

    private class ContextStack : List<IDisposable>
    {
      public void Push(IDisposable o)
      {
        Add(o);
      }

      public IDisposable Pop()
      {
        IDisposable current = Current;
        if (Count > 0)
          RemoveAt(Count - 1);
        return current;
      }

      internal IDisposable Current
      {
        get
        {
          if (Count <= 0)
            return (IDisposable) null;
          return this[Count - 1];
        }
        set
        {
          this[Count - 1] = value;
        }
      }
    }
  }
}
