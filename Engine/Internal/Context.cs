using System;
using System.Collections.Generic;
using System.Threading;

namespace VistaDB.Engine.Internal
{
  internal class Context
  {
    private Dictionary<int, Context.ContextStack> threadsContext = new Dictionary<int, Context.ContextStack>();
    private bool active = true;

    private static int ThreadId
    {
      get
      {
        return Thread.CurrentThread.ManagedThreadId;
      }
    }

    private Context.ContextStack GetStack()
    {
      if (this.threadsContext.ContainsKey(Context.ThreadId))
        return this.threadsContext[Context.ThreadId];
      Context.ContextStack contextStack = new Context.ContextStack();
      this.threadsContext.Add(Context.ThreadId, contextStack);
      return contextStack;
    }

    private void ReleaseStack()
    {
      this.threadsContext.Remove(Context.ThreadId);
    }

    internal bool Available
    {
      get
      {
        if (this.active)
          return this.CurrentContext != null;
        return false;
      }
    }

    internal IDisposable CurrentContext
    {
      get
      {
        lock (this.threadsContext)
        {
          if (!this.active)
            return (IDisposable) null;
          return this.GetStack().Current;
        }
      }
      set
      {
        lock (this.threadsContext)
          this.GetStack().Current = value;
      }
    }

    internal void PushContext(IDisposable newContext)
    {
      lock (this.threadsContext)
        this.GetStack().Push(newContext);
    }

    internal void PopContext()
    {
      lock (this.threadsContext)
      {
        Context.ContextStack stack = this.GetStack();
        try
        {
          stack.Pop()?.Dispose();
        }
        finally
        {
          if (stack.Count == 0)
            this.ReleaseStack();
        }
      }
    }

    private class ContextStack : List<IDisposable>
    {
      public void Push(IDisposable o)
      {
        this.Add(o);
      }

      public IDisposable Pop()
      {
        IDisposable current = this.Current;
        if (this.Count > 0)
          this.RemoveAt(this.Count - 1);
        return current;
      }

      internal IDisposable Current
      {
        get
        {
          if (this.Count <= 0)
            return (IDisposable) null;
          return this[this.Count - 1];
        }
        set
        {
          this[this.Count - 1] = value;
        }
      }
    }
  }
}
