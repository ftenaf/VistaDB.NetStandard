using System;
using System.Collections.Generic;

namespace VistaDB.Engine.Internal
{
  internal class WeakReferenceCache<TKey, TValue> where TValue : class
  {
    private Dictionary<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>> m_Cache;
    private WeakReferenceCache<TKey, TValue>.LRUQueue<TValue> m_Queue;
    private readonly int m_InitialCapacity;
    private int m_AddCount;
    private TKey m_PreviousKey;
    private TValue m_PreviousValue;
    private readonly IEqualityComparer<TKey> m_Comparer;

    public WeakReferenceCache(int capacity)
      : this(capacity, (IEqualityComparer<TKey>) null)
    {
    }

    public WeakReferenceCache(int capacity, IEqualityComparer<TKey> comparer)
    {
      this.m_Comparer = comparer ?? (IEqualityComparer<TKey>) EqualityComparer<TKey>.Default;
      this.m_Cache = new Dictionary<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>>(capacity, this.m_Comparer);
      this.m_Queue = new WeakReferenceCache<TKey, TValue>.LRUQueue<TValue>(capacity);
      this.m_InitialCapacity = capacity;
    }

    public TValue this[TKey key]
    {
      get
      {
        if ((object) this.m_PreviousValue != null && this.m_Comparer.Equals(key, this.m_PreviousKey))
          return this.m_PreviousValue;
        TValue obj = default (TValue);
        WeakReferenceCache<TKey, TValue>.CacheNode<TValue> cacheNode;
        if (this.m_Cache.TryGetValue(key, out cacheNode))
        {
          obj = cacheNode.Value;
          if ((object) obj == null)
          {
            this.m_Queue.Remove(cacheNode.QueueNode);
            cacheNode.QueueNode = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
            this.m_Cache.Remove(key);
          }
          else if (cacheNode.QueueNode == null || (object) cacheNode.QueueNode.Value == null)
            cacheNode.QueueNode = this.m_Queue.AddToFront(obj);
          else
            this.m_Queue.MoveToFront(cacheNode.QueueNode);
        }
        if ((object) obj == null)
        {
          obj = this.FetchValue(key);
          if ((object) obj != null)
            this[key] = obj;
        }
        if ((object) obj != null)
        {
          this.m_PreviousKey = key;
          this.m_PreviousValue = obj;
        }
        return obj;
      }
      set
      {
        WeakReferenceCache<TKey, TValue>.LRUNode<TValue> front = this.m_Queue.AddToFront(value);
        WeakReferenceCache<TKey, TValue>.CacheNode<TValue> cacheNode = new WeakReferenceCache<TKey, TValue>.CacheNode<TValue>() { Value = value, QueueNode = front };
        this.m_Cache[key] = cacheNode;
        if (this.m_Comparer.Equals(key, this.m_PreviousKey))
          this.m_PreviousValue = value;
        if (++this.m_AddCount <= this.m_InitialCapacity)
          return;
        this.Pack(false);
        this.m_AddCount = 0;
      }
    }

    public void AddToWeakCache(TKey key, TValue newValue)
    {
      WeakReferenceCache<TKey, TValue>.CacheNode<TValue> cacheNode1;
      if (this.m_Cache.TryGetValue(key, out cacheNode1) && (object) cacheNode1.Value == null)
      {
        this.m_Queue.Remove(cacheNode1.QueueNode);
        cacheNode1.QueueNode = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        this.m_Cache.Remove(key);
      }
      WeakReferenceCache<TKey, TValue>.CacheNode<TValue> cacheNode2 = new WeakReferenceCache<TKey, TValue>.CacheNode<TValue>() { Value = newValue, QueueNode = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null };
      this.m_Cache.Add(key, cacheNode2);
      if (++this.m_AddCount <= this.m_InitialCapacity)
        return;
      this.Pack(false);
      this.m_AddCount = 0;
    }

    public bool Remove(TKey key)
    {
      WeakReferenceCache<TKey, TValue>.CacheNode<TValue> cacheNode;
      if (!this.m_Cache.TryGetValue(key, out cacheNode))
        return false;
      WeakReferenceCache<TKey, TValue>.LRUNode<TValue> queueNode = cacheNode.QueueNode;
      TValue obj = cacheNode.Value;
      if (queueNode != null)
        this.m_Queue.Remove(queueNode);
      if ((object) this.m_PreviousValue != null && this.m_Comparer.Equals(key, this.m_PreviousKey))
      {
        this.m_PreviousValue = default (TValue);
        this.m_PreviousKey = default (TKey);
      }
      this.m_Cache.Remove(key);
      return (object) obj != null;
    }

    public List<TValue> GetMostRecentList()
    {
      return this.m_Queue.GetValues(false);
    }

    public List<TValue> GetMostRecentList(bool reversed)
    {
      return this.m_Queue.GetValues(reversed);
    }

    public IEnumerator<TValue> EnumerateMostRecent()
    {
      return (IEnumerator<TValue>) this.m_Queue.GetValues().GetEnumerator();
    }

    public IEnumerator<TValue> EnumerateEntireCache()
    {
      foreach (WeakReferenceCache<TKey, TValue>.CacheNode<TValue> cacheNode in this.m_Cache.Values)
      {
        TValue value = cacheNode.Value;
        if ((object) value != null)
          yield return value;
      }
    }

    public TValue GetLeastRecentValue()
    {
      if (this.m_Queue.Count < this.m_InitialCapacity)
        return default (TValue);
      return this.m_Queue.GetLast();
    }

    public void Clear()
    {
      this.m_Cache = new Dictionary<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>>(this.m_InitialCapacity, this.m_Comparer);
      this.m_Queue = new WeakReferenceCache<TKey, TValue>.LRUQueue<TValue>(this.m_InitialCapacity);
      this.m_AddCount = 0;
      this.m_PreviousValue = default (TValue);
      this.m_PreviousKey = default (TKey);
    }

    public void Pack(bool forcePack)
    {
      int liveReferenceCount = this.LiveReferenceCount;
      int num = this.m_Cache.Count - liveReferenceCount;
      if (num == 0 || !forcePack && num < liveReferenceCount)
        return;
      Dictionary<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>> dictionary = new Dictionary<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>>((liveReferenceCount / this.m_InitialCapacity + 1) * this.m_InitialCapacity);
      foreach (KeyValuePair<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>> keyValuePair in this.m_Cache)
      {
        if (keyValuePair.Value.WeakReference.IsAlive)
          dictionary[keyValuePair.Key] = keyValuePair.Value;
      }
      this.m_Cache = dictionary;
    }

    public virtual TValue FetchValue(TKey key)
    {
      return default (TValue);
    }

    public int CacheCount
    {
      get
      {
        return this.m_Cache.Count;
      }
    }

    public int QueueCount
    {
      get
      {
        return this.m_Queue.Count;
      }
    }

    public int LiveReferenceCount
    {
      get
      {
        int num = 0;
        foreach (KeyValuePair<TKey, WeakReferenceCache<TKey, TValue>.CacheNode<TValue>> keyValuePair in this.m_Cache)
        {
          if (keyValuePair.Value.WeakReference.IsAlive)
            ++num;
        }
        return num;
      }
    }

    public class CacheNode<TValue> where TValue : class
    {
      public WeakReference WeakReference { get; set; }

      public WeakReferenceCache<TKey, TValue>.LRUNode<TValue> QueueNode { get; set; }

      public TValue Value
      {
        get
        {
          if (this.WeakReference == null)
            return default (TValue);
          TValue target = (TValue) this.WeakReference.Target;
          if (!this.WeakReference.IsAlive)
            return default (TValue);
          return target;
        }
        set
        {
          this.WeakReference = new WeakReference((object) value);
        }
      }
    }

    public class LRUQueue<TValue> where TValue : class
    {
      private readonly int m_Capacity;

      private WeakReferenceCache<TKey, TValue>.LRUNode<TValue> Head { get; set; }

      private WeakReferenceCache<TKey, TValue>.LRUNode<TValue> Tail { get; set; }

      public int Count { get; private set; }

      public LRUQueue(int capacity)
      {
        this.m_Capacity = capacity;
      }

      public TValue GetFirst()
      {
        if (this.Head != null)
          return this.Head.Value;
        return default (TValue);
      }

      public TValue GetLast()
      {
        if (this.Tail != null)
          return this.Tail.Value;
        return default (TValue);
      }

      public WeakReferenceCache<TKey, TValue>.LRUNode<TValue> AddToFront(TValue value)
      {
        WeakReferenceCache<TKey, TValue>.LRUNode<TValue> lruNode = new WeakReferenceCache<TKey, TValue>.LRUNode<TValue>() { Value = value, Next = this.Head, Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null };
        if (this.Head != null)
          this.Head.Previous = lruNode;
        this.Head = lruNode;
        if (this.Tail == null)
          this.Tail = lruNode;
        ++this.Count;
        if (this.Count > this.m_Capacity)
          this.RemoveFromBack();
        return lruNode;
      }

      public void MoveToFront(WeakReferenceCache<TKey, TValue>.LRUNode<TValue> node)
      {
        if (this.Head == node)
          return;
        if (node.Previous != null)
          node.Previous.Next = node.Next;
        if (node.Next != null)
          node.Next.Previous = node.Previous;
        if (this.Tail == node)
          this.Tail = node.Previous;
        node.Next = this.Head;
        this.Head.Previous = node;
        node.Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        this.Head = node;
      }

      public void RemoveFromBack()
      {
        WeakReferenceCache<TKey, TValue>.LRUNode<TValue> tail = this.Tail;
        if (tail == null)
          return;
        this.Tail = tail.Previous;
        if (this.Tail != null)
          this.Tail.Next = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        tail.Next = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        tail.Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        tail.Value = default (TValue);
        --this.Count;
      }

      public void Remove(WeakReferenceCache<TKey, TValue>.LRUNode<TValue> node)
      {
        if (node == null || (object) node.Value == null)
          return;
        if (node.Previous != null)
          node.Previous.Next = node.Next;
        if (node.Next != null)
          node.Next.Previous = node.Previous;
        if (this.Head == node)
          this.Head = node.Next;
        if (this.Tail == node)
          this.Tail = node.Previous;
        node.Next = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        node.Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>) null;
        node.Value = default (TValue);
        --this.Count;
      }

      public List<TValue> GetValues()
      {
        return this.GetValues(false);
      }

      public List<TValue> GetValues(bool reversed)
      {
        List<TValue> objList = new List<TValue>(this.Count);
        if (reversed)
        {
          for (WeakReferenceCache<TKey, TValue>.LRUNode<TValue> lruNode = this.Tail; lruNode != null; lruNode = lruNode.Previous)
            objList.Add(lruNode.Value);
        }
        else
        {
          for (WeakReferenceCache<TKey, TValue>.LRUNode<TValue> lruNode = this.Head; lruNode != null; lruNode = lruNode.Next)
            objList.Add(lruNode.Value);
        }
        return objList;
      }
    }

    public class LRUNode<TValue> where TValue : class
    {
      public WeakReferenceCache<TKey, TValue>.LRUNode<TValue> Previous { get; set; }

      public WeakReferenceCache<TKey, TValue>.LRUNode<TValue> Next { get; set; }

      public TValue Value { get; set; }
    }

    internal class MruList
    {
      private readonly int m_Capacity;
      private readonly LinkedList<TValue> m_MruList;

      internal MruList(int capacity)
      {
        this.m_Capacity = capacity;
        this.m_MruList = new LinkedList<TValue>();
      }

      public TValue GetFirst()
      {
        if (this.m_MruList.First != null)
          return this.m_MruList.First.Value;
        return default (TValue);
      }

      public TValue GetLast()
      {
        if (this.m_MruList.Last != null)
          return this.m_MruList.Last.Value;
        return default (TValue);
      }

      internal LinkedListNode<TValue> AddToFront(TValue value)
      {
        LinkedListNode<TValue> linkedListNode = this.m_MruList.AddFirst(value);
        if (this.m_MruList.Count > this.m_Capacity)
          this.m_MruList.RemoveLast();
        return linkedListNode;
      }

      internal void MoveToFront(LinkedListNode<TValue> node)
      {
        if (node.List == null)
        {
          this.m_MruList.AddFirst(node);
          if (this.m_MruList.Count > this.m_Capacity)
            this.m_MruList.RemoveLast();
        }
        if (node.List != this.m_MruList || object.ReferenceEquals((object) this.m_MruList.First, (object) node))
          return;
        this.m_MruList.Remove(node);
        this.m_MruList.AddFirst(node);
      }

      internal void RemoveFromBack()
      {
        if (this.m_MruList.Count == 0)
          return;
        LinkedListNode<TValue> last = this.m_MruList.Last;
        if (last == null)
          return;
        this.m_MruList.Remove(last);
      }

      internal void Remove(LinkedListNode<TValue> node)
      {
        if (node == null || node.List == null || node.List != this.m_MruList)
          return;
        this.m_MruList.Remove(node);
      }

      public List<TValue> GetValues()
      {
        return this.GetValues(false);
      }

      internal List<TValue> GetValues(bool reversed)
      {
        List<TValue> objList = new List<TValue>(this.m_MruList.Count);
        if (reversed)
        {
          for (LinkedListNode<TValue> linkedListNode = this.m_MruList.Last; linkedListNode != null; linkedListNode = linkedListNode.Previous)
            objList.Add(linkedListNode.Value);
        }
        else
        {
          for (LinkedListNode<TValue> linkedListNode = this.m_MruList.First; linkedListNode != null; linkedListNode = linkedListNode.Next)
            objList.Add(linkedListNode.Value);
        }
        return objList;
      }
    }
  }
}
