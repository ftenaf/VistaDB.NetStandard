using System;
using System.Collections.Generic;

namespace VistaDB.Engine.Internal
{
    internal class WeakReferenceCache<TKey, TValue> where TValue : class
    {
        private Dictionary<TKey, CacheNode<TValue>> m_Cache;
        private LRUQueue<TValue> m_Queue;
        private readonly int m_InitialCapacity;
        private int m_AddCount;
        private TKey m_PreviousKey;
        private TValue m_PreviousValue;
        private readonly IEqualityComparer<TKey> m_Comparer;

        public WeakReferenceCache(int capacity)
          : this(capacity, (IEqualityComparer<TKey>)null)
        {
        }

        public WeakReferenceCache(int capacity, IEqualityComparer<TKey> comparer)
        {
            m_Comparer = comparer ?? (IEqualityComparer<TKey>)EqualityComparer<TKey>.Default;
            m_Cache = new Dictionary<TKey, CacheNode<TValue>>(capacity, m_Comparer);
            m_Queue = new LRUQueue<TValue>(capacity);
            m_InitialCapacity = capacity;
        }

        public TValue this[TKey key]
        {
            get
            {
                if ((object)m_PreviousValue != null && m_Comparer.Equals(key, m_PreviousKey))
                    return m_PreviousValue;
                TValue obj = default(TValue);
                CacheNode<TValue> cacheNode;
                if (m_Cache.TryGetValue(key, out cacheNode))
                {
                    obj = cacheNode.Value;
                    if ((object)obj == null)
                    {
                        m_Queue.Remove(cacheNode.QueueNode);
                        cacheNode.QueueNode = (LRUNode<TValue>)null;
                        m_Cache.Remove(key);
                    }
                    else if (cacheNode.QueueNode == null || (object)cacheNode.QueueNode.Value == null)
                        cacheNode.QueueNode = m_Queue.AddToFront(obj);
                    else
                        m_Queue.MoveToFront(cacheNode.QueueNode);
                }
                if ((object)obj == null)
                {
                    obj = FetchValue(key);
                    if ((object)obj != null)
                        this[key] = obj;
                }
                if ((object)obj != null)
                {
                    m_PreviousKey = key;
                    m_PreviousValue = obj;
                }
                return obj;
            }
            set
            {
                LRUNode<TValue> front = m_Queue.AddToFront(value);
                CacheNode<TValue> cacheNode = new CacheNode<TValue>() { Value = value, QueueNode = front };
                m_Cache[key] = cacheNode;
                if (m_Comparer.Equals(key, m_PreviousKey))
                    m_PreviousValue = value;
                if (++m_AddCount <= m_InitialCapacity)
                    return;
                Pack(false);
                m_AddCount = 0;
            }
        }

        public void AddToWeakCache(TKey key, TValue newValue)
        {
            CacheNode<TValue> cacheNode1;
            if (m_Cache.TryGetValue(key, out cacheNode1) && (object)cacheNode1.Value == null)
            {
                m_Queue.Remove(cacheNode1.QueueNode);
                cacheNode1.QueueNode = (LRUNode<TValue>)null;
                m_Cache.Remove(key);
            }
            CacheNode<TValue> cacheNode2 = new CacheNode<TValue>() { Value = newValue, QueueNode = (LRUNode<TValue>)null };
            m_Cache.Add(key, cacheNode2);
            if (++m_AddCount <= m_InitialCapacity)
                return;
            Pack(false);
            m_AddCount = 0;
        }

        public bool Remove(TKey key)
        {
            CacheNode<TValue> cacheNode;
            if (!m_Cache.TryGetValue(key, out cacheNode))
                return false;
            LRUNode<TValue> queueNode = cacheNode.QueueNode;
            TValue obj = cacheNode.Value;
            if (queueNode != null)
                m_Queue.Remove(queueNode);
            if ((object)m_PreviousValue != null && m_Comparer.Equals(key, m_PreviousKey))
            {
                m_PreviousValue = default(TValue);
                m_PreviousKey = default(TKey);
            }
            m_Cache.Remove(key);
            return (object)obj != null;
        }

        public List<TValue> GetMostRecentList()
        {
            return m_Queue.GetValues(false);
        }

        public List<TValue> GetMostRecentList(bool reversed)
        {
            return m_Queue.GetValues(reversed);
        }

        public IEnumerator<TValue> EnumerateMostRecent()
        {
            return (IEnumerator<TValue>)m_Queue.GetValues().GetEnumerator();
        }

        public IEnumerator<TValue> EnumerateEntireCache()
        {
            foreach (CacheNode<TValue> cacheNode in m_Cache.Values)
            {
                TValue value = cacheNode.Value;
                if ((object)value != null)
                    yield return value;
            }
        }

        public TValue GetLeastRecentValue()
        {
            if (m_Queue.Count < m_InitialCapacity)
                return default(TValue);
            return m_Queue.GetLast();
        }

        public void Clear()
        {
            m_Cache = new Dictionary<TKey, CacheNode<TValue>>(m_InitialCapacity, m_Comparer);
            m_Queue = new LRUQueue<TValue>(m_InitialCapacity);
            m_AddCount = 0;
            m_PreviousValue = default(TValue);
            m_PreviousKey = default(TKey);
        }

        public void Pack(bool forcePack)
        {
            int liveReferenceCount = LiveReferenceCount;
            int num = m_Cache.Count - liveReferenceCount;
            if (num == 0 || !forcePack && num < liveReferenceCount)
                return;
            Dictionary<TKey, CacheNode<TValue>> dictionary = new Dictionary<TKey, CacheNode<TValue>>((liveReferenceCount / m_InitialCapacity + 1) * m_InitialCapacity);
            foreach (KeyValuePair<TKey, CacheNode<TValue>> keyValuePair in m_Cache)
            {
                if (keyValuePair.Value.WeakReference.IsAlive)
                    dictionary[keyValuePair.Key] = keyValuePair.Value;
            }
            m_Cache = dictionary;
        }

        public virtual TValue FetchValue(TKey key)
        {
            return default(TValue);
        }

        public int CacheCount
        {
            get
            {
                return m_Cache.Count;
            }
        }

        public int QueueCount
        {
            get
            {
                return m_Queue.Count;
            }
        }

        public int LiveReferenceCount
        {
            get
            {
                int num = 0;
                foreach (KeyValuePair<TKey, CacheNode<TValue>> keyValuePair in m_Cache)
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
                    if (WeakReference == null)
                        return default(TValue);
                    TValue target = (TValue)WeakReference.Target;
                    if (!WeakReference.IsAlive)
                        return default(TValue);
                    return target;
                }
                set
                {
                    WeakReference = new WeakReference((object)value);
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
                m_Capacity = capacity;
            }

            public TValue GetFirst()
            {
                if (Head != null)
                    return Head.Value;
                return default(TValue);
            }

            public TValue GetLast()
            {
                if (Tail != null)
                    return Tail.Value;
                return default(TValue);
            }

            public WeakReferenceCache<TKey, TValue>.LRUNode<TValue> AddToFront(TValue value)
            {
                WeakReferenceCache<TKey, TValue>.LRUNode<TValue> lruNode = new WeakReferenceCache<TKey, TValue>.LRUNode<TValue>() { Value = value, Next = Head, Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null };
                if (Head != null)
                    Head.Previous = lruNode;
                Head = lruNode;
                if (Tail == null)
                    Tail = lruNode;
                ++Count;
                if (Count > m_Capacity)
                    RemoveFromBack();
                return lruNode;
            }

            public void MoveToFront(WeakReferenceCache<TKey, TValue>.LRUNode<TValue> node)
            {
                if (Head == node)
                    return;
                if (node.Previous != null)
                    node.Previous.Next = node.Next;
                if (node.Next != null)
                    node.Next.Previous = node.Previous;
                if (Tail == node)
                    Tail = node.Previous;
                node.Next = Head;
                Head.Previous = node;
                node.Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null;
                Head = node;
            }

            public void RemoveFromBack()
            {
                WeakReferenceCache<TKey, TValue>.LRUNode<TValue> tail = Tail;
                if (tail == null)
                    return;
                Tail = tail.Previous;
                if (Tail != null)
                    Tail.Next = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null;
                tail.Next = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null;
                tail.Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null;
                tail.Value = default(TValue);
                --Count;
            }

            public void Remove(WeakReferenceCache<TKey, TValue>.LRUNode<TValue> node)
            {
                if (node == null || (object)node.Value == null)
                    return;
                if (node.Previous != null)
                    node.Previous.Next = node.Next;
                if (node.Next != null)
                    node.Next.Previous = node.Previous;
                if (Head == node)
                    Head = node.Next;
                if (Tail == node)
                    Tail = node.Previous;
                node.Next = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null;
                node.Previous = (WeakReferenceCache<TKey, TValue>.LRUNode<TValue>)null;
                node.Value = default(TValue);
                --Count;
            }

            public List<TValue> GetValues()
            {
                return GetValues(false);
            }

            public List<TValue> GetValues(bool reversed)
            {
                List<TValue> objList = new List<TValue>(Count);
                if (reversed)
                {
                    for (WeakReferenceCache<TKey, TValue>.LRUNode<TValue> lruNode = Tail; lruNode != null; lruNode = lruNode.Previous)
                        objList.Add(lruNode.Value);
                }
                else
                {
                    for (WeakReferenceCache<TKey, TValue>.LRUNode<TValue> lruNode = Head; lruNode != null; lruNode = lruNode.Next)
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
                m_Capacity = capacity;
                m_MruList = new LinkedList<TValue>();
            }

            public TValue GetFirst()
            {
                if (m_MruList.First != null)
                    return m_MruList.First.Value;
                return default(TValue);
            }

            public TValue GetLast()
            {
                if (m_MruList.Last != null)
                    return m_MruList.Last.Value;
                return default(TValue);
            }

            internal LinkedListNode<TValue> AddToFront(TValue value)
            {
                LinkedListNode<TValue> linkedListNode = m_MruList.AddFirst(value);
                if (m_MruList.Count > m_Capacity)
                    m_MruList.RemoveLast();
                return linkedListNode;
            }

            internal void MoveToFront(LinkedListNode<TValue> node)
            {
                if (node.List == null)
                {
                    m_MruList.AddFirst(node);
                    if (m_MruList.Count > m_Capacity)
                        m_MruList.RemoveLast();
                }
                if (node.List != m_MruList || ReferenceEquals((object)m_MruList.First, (object)node))
                    return;
                m_MruList.Remove(node);
                m_MruList.AddFirst(node);
            }

            internal void RemoveFromBack()
            {
                if (m_MruList.Count == 0)
                    return;
                LinkedListNode<TValue> last = m_MruList.Last;
                if (last == null)
                    return;
                m_MruList.Remove(last);
            }

            internal void Remove(LinkedListNode<TValue> node)
            {
                if (node == null || node.List == null || node.List != m_MruList)
                    return;
                m_MruList.Remove(node);
            }

            public List<TValue> GetValues()
            {
                return GetValues(false);
            }

            internal List<TValue> GetValues(bool reversed)
            {
                List<TValue> objList = new List<TValue>(m_MruList.Count);
                if (reversed)
                {
                    for (LinkedListNode<TValue> linkedListNode = m_MruList.Last; linkedListNode != null; linkedListNode = linkedListNode.Previous)
                        objList.Add(linkedListNode.Value);
                }
                else
                {
                    for (LinkedListNode<TValue> linkedListNode = m_MruList.First; linkedListNode != null; linkedListNode = linkedListNode.Next)
                        objList.Add(linkedListNode.Value);
                }
                return objList;
            }
        }
    }
}
