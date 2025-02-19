﻿namespace VistaDB.Engine.Internal
{
  internal class SynchronizedWeakReferenceCache<TKey, TValue> : WeakReferenceCache<TKey, TValue> where TValue : class
  {
    private readonly object m_SyncLock = new object();

    public SynchronizedWeakReferenceCache(int capacity)
      : base(capacity)
    {
    }

    public new TValue this[TKey key]
    {
      get
      {
        lock (m_SyncLock)
          return base[key];
      }
      set
      {
        lock (m_SyncLock)
          base[key] = value;
      }
    }

    public new void Clear()
    {
      lock (m_SyncLock)
        base.Clear();
    }

    public new void Pack(bool forcePack)
    {
      lock (m_SyncLock)
        base.Pack(forcePack);
    }
  }
}
