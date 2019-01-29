using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.Indexing
{
  internal class Tree : Dictionary<ulong, Node>, IDisposable
  {
    private ulong rootNodeId = Row.EmptyReference;
    private ulong currentNodeId = Row.EmptyReference;
    private int clearingCount = -1;
    private readonly NodeCache nodeCache = new NodeCache();
    private readonly List<Node> parentNodeChain = new List<Node>();
    private Index parentIndex;
    private int pageSize;
    private bool modified;
    private int depth;
    private int cloneCounter;
    private bool isDisposed;

    internal Tree(Index parentIndex)
    {
      this.parentIndex = parentIndex;
      this.pageSize = parentIndex.PageSize;
    }

    internal Row CurrentKey
    {
      get
      {
        return this.GetCurrentKey();
      }
    }

    internal Node CurrentNode
    {
      get
      {
        return this.GetNodeAtPosition(this.currentNodeId);
      }
      set
      {
        this.currentNodeId = value.Id;
      }
    }

    internal Node RootNode
    {
      get
      {
        return this.GetNodeAtPosition(this.rootNodeId);
      }
    }

    internal bool Modified
    {
      get
      {
        return this.modified;
      }
      set
      {
        this.modified = value;
      }
    }

    internal Index ParentIndex
    {
      get
      {
        return this.parentIndex;
      }
    }

    internal new int Count
    {
      get
      {
        return base.Count + this.nodeCache.LiveReferenceCount;
      }
    }

    internal bool IsCacheEmpty
    {
      get
      {
        int num = base.Count + this.nodeCache.QueueCount;
        if (num == 0)
          num = this.nodeCache.LiveReferenceCount;
        return num == 0;
      }
    }

    private Node CreateNodeInstance(ulong filePosition)
    {
      Node nodeInstance = this.OnCreateNodeInstance(filePosition, 0);
      this.nodeCache.AddToWeakCache(filePosition, nodeInstance);
      return nodeInstance;
    }

    private Node CreateNodeInstance(ulong filePosition, int levelIndex)
    {
      Node nodeInstance = this.OnCreateNodeInstance(filePosition, levelIndex);
      this.nodeCache.AddToWeakCache(filePosition, nodeInstance);
      return nodeInstance;
    }

    internal void GoRoot()
    {
      this.currentNodeId = this.rootNodeId;
      this.CurrentNode.KeyIndex = -1;
    }

    private Node FindNode(ulong nodeId)
    {
      Node node;
      if (!this.TryGetValue(nodeId, out node))
        node = this.nodeCache[nodeId];
      return node;
    }

    private Row GetCurrentKey()
    {
      Node currentNode = this.CurrentNode;
      return currentNode[currentNode.KeyIndex];
    }

    internal void CleanUpNodeSpace(Node node, bool cleanCurrent)
    {
      if (node == null)
        return;
      node.UnpackInformation();
      node.SetModified(false);
      for (int keyIndex = 0; keyIndex < node.KeyCount; ++keyIndex)
      {
        node[keyIndex].FreeExtensionSpace((DataStorage) this.ParentIndex);
        this.CleanUpNodeSpace(node.GetChildNode(keyIndex), true);
      }
      if (!cleanCurrent)
        return;
      node.ClearApartment();
      this.RemoveNode(node.Id);
      node.Dispose();
    }

    internal void ModifiedNode(Node node)
    {
      this.modified = true;
      if (this.ContainsKey(node.Id) || node.IsDisposed)
        return;
      this.Add(node.Id, node);
    }

    private Node GetNodeAtLevel(int level)
    {
      if (level < this.parentNodeChain.Count)
        return this.parentNodeChain[level];
      if (level != this.parentNodeChain.Count)
        return (Node) null;
      Node node = this.CreateNode(level == 0 ? Node.NodeType.Leaf : Node.NodeType.None, level);
      this.parentNodeChain.Add(node);
      return node;
    }

    private Node AppendKey(Row newKey, Node levelNode)
    {
      int treeLevel = levelNode.TreeLevel;
      levelNode.AppendedLength += newKey.GetMemoryApartment(levelNode.PrecedenceKey);
      levelNode.PrecedenceKey = newKey;
      if (levelNode.IsSplitRequired(levelNode.AppendedLength))
      {
        Node.NodeType nodeType = treeLevel == 0 ? Node.NodeType.Leaf : Node.NodeType.None;
        Node node = this.CreateNode(nodeType, treeLevel);
        this.parentNodeChain[treeLevel] = node;
        try
        {
          node.LeftNodeId = levelNode.Id;
          levelNode.RightNodeId = node.Id;
          levelNode.Type = nodeType;
          Row newKey1 = levelNode[0].CopyInstance();
          newKey1.RefPosition = levelNode.Id;
          levelNode.Flush(true);
          this.RemoveNode(levelNode.Id);
          this.AppendKey(newKey1, this.GetNodeAtLevel(treeLevel + 1));
        }
        finally
        {
          levelNode.Dispose();
        }
        this.AppendKey(newKey, node);
        return node;
      }
      levelNode.Add(newKey);
      levelNode.KeyCount = levelNode.Count;
      return levelNode;
    }

    protected virtual Node OnCreateNodeInstance(ulong filePosition, int level)
    {
      return Node.CreateInstance(filePosition, level, this, this.parentIndex.TopRow, false, this.parentIndex.PageSize);
    }

    protected virtual Node OnGetNodeAtPosition(ulong filePosition)
    {
      Node node1 = this.FindNode(filePosition);
      if (node1 == null)
      {
        node1 = this.CreateNodeInstance(filePosition);
        try
        {
          node1.Update();
        }
        catch
        {
          throw;
        }
        if (!this.ContainsKey(filePosition))
        {
          Node node2 = this.nodeCache[filePosition];
        }
      }
      return node1;
    }

    public new void Clear()
    {
      this.modified = false;
      this.depth = 0;
      this.rootNodeId = Row.EmptyReference;
      this.currentNodeId = Row.EmptyReference;
      base.Clear();
      this.nodeCache.Clear();
      this.parentNodeChain.Clear();
    }

    internal Tree GetClone()
    {
      ++this.cloneCounter;
      return this;
    }

    internal void FlushTree()
    {
      if (!this.modified && base.Count == 0)
        return;
      while (base.Count > 0)
      {
        foreach (Node node in new List<Node>((IEnumerable<Node>) this.Values))
        {
          if (node.Modified)
            node.Flush(false);
          if (!node.Modified)
            this.Remove(node.Id);
        }
      }
      base.Clear();
      this.modified = false;
    }

    internal bool MinimizeTreeMemory(bool forceClearing)
    {
      int num = 10;
      if (forceClearing)
        num = 1;
      if (base.Count <= num)
        return true;
      this.FlushTree();
      return true;
    }

    internal void ActivateRoot(ulong rootPosition)
    {
      this.rootNodeId = rootPosition;
      Node nodeAtPosition = this.GetNodeAtPosition(rootPosition);
      if (nodeAtPosition == null)
        throw new VistaDBException(139, rootPosition.ToString());
      this.currentNodeId = rootPosition;
      nodeAtPosition.KeyIndex = 0;
    }

    internal void CreateRoot()
    {
      Node node = this.CreateNode(Node.NodeType.Root | Node.NodeType.Leaf);
      this.rootNodeId = node.Id;
      this.parentNodeChain.Add(node);
      this.currentNodeId = this.rootNodeId;
      node.KeyIndex = 0;
    }

    internal Node CreateNode(Node.NodeType nodeType, int levelIndex)
    {
      ulong freeCluster = this.parentIndex.GetFreeCluster(1);
      Node nodeInstance = this.CreateNodeInstance(freeCluster, levelIndex);
      this.Add(freeCluster, nodeInstance);
      this.currentNodeId = freeCluster;
      nodeInstance.Type = nodeType;
      return nodeInstance;
    }

    internal Node CreateNode(Node.NodeType nodeType)
    {
      ulong freeCluster = this.parentIndex.GetFreeCluster(1);
      Node nodeInstance = this.CreateNodeInstance(freeCluster);
      this.Add(freeCluster, nodeInstance);
      this.currentNodeId = freeCluster;
      nodeInstance.Type = nodeType;
      return nodeInstance;
    }

    internal bool ReplaceKey(Row oldKey, Row newKey, uint transactionId)
    {
      Node node1;
      bool flag;
      int index;
      if ((int) oldKey.RowId != (int) Row.MinRowId && (int) oldKey.RowId != (int) Row.MaxRowId)
      {
        node1 = this.GoKey(oldKey, (Node) null);
        flag = node1.KeyRank == Node.KeyPosition.Equal;
        index = node1.KeyIndex;
        if (transactionId != 0U && flag && ((int) oldKey.TransactionId != (int) transactionId || oldKey.OutdatedStatus))
        {
          Row newKey1 = oldKey.CopyInstance();
          newKey1.RowVersion = transactionId;
          newKey1.OutdatedStatus = true;
          this.ReplaceKey(oldKey, newKey1, 0U);
          flag = false;
        }
      }
      else
      {
        node1 = (Node) null;
        flag = false;
        index = -1;
      }
      Node node2 = this.GoKey(newKey, (Node) null);
      int keyIndex = node2.KeyIndex;
      try
      {
        if (node2.KeyRank == Node.KeyPosition.OnRight && node2.KeyCount > 0)
          ++keyIndex;
        node2.InsertKey(keyIndex, newKey, Row.EmptyReference);
        if (node2 == node1 && keyIndex <= index)
          ++index;
        return true;
      }
      finally
      {
        if (flag)
          node1.DeleteKey(index);
        this.currentNodeId = node2.Id;
        node2.KeyIndex = -1;
      }
    }

    internal bool DeleteKey(Row oldKey, uint transactionId)
    {
      Node node = this.GoKey(oldKey, (Node) null);
      if (node.KeyRank != Node.KeyPosition.Equal)
        return false;
      if (transactionId != 0U && ((int) oldKey.TransactionId != (int) transactionId || oldKey.OutdatedStatus))
      {
        Row newKey = oldKey.CopyInstance();
        newKey.RowVersion = transactionId;
        newKey.OutdatedStatus = true;
        this.ReplaceKey(oldKey, newKey, 0U);
        if (node.KeyCount > 0)
          this.currentNodeId = node.Id;
        ++node.KeyIndex;
      }
      else
      {
        node = node.DeleteKey(node.KeyIndex);
        this.currentNodeId = node.Id;
      }
      if (node.KeyIndex == node.KeyCount)
        this.GoNextKey();
      return true;
    }

    internal Node GoKey(Row key, Node lowestNode)
    {
      int num = 0;
      Node node1 = this.RootNode;
      Node node2;
      do
      {
        node2 = node1;
        int keyIndex = node2.GoNodeKey(key);
        node1 = node2.GetChildNode(keyIndex);
        ++num;
      }
      while (node1 != lowestNode && node1 != null);
      if (lowestNode == null)
        this.depth = num;
      this.currentNodeId = node2.Id;
      return node2;
    }

    internal void GoNextKey()
    {
      Node currentNode = this.CurrentNode;
      ++currentNode.KeyIndex;
      if (currentNode.KeyIndex < currentNode.Count)
        return;
      currentNode.KeyIndex = currentNode.Count;
      Node rightNode = currentNode.GetRightNode();
      if (rightNode == null)
        return;
      this.currentNodeId = rightNode.Id;
      rightNode.KeyIndex = 0;
    }

    internal void GoPrevKey()
    {
      Node currentNode = this.CurrentNode;
      --currentNode.KeyIndex;
      if (currentNode.KeyIndex >= 0)
        return;
      currentNode.KeyIndex = -1;
      Node leftNode = currentNode.GetLeftNode();
      if (leftNode == null)
        return;
      this.currentNodeId = leftNode.Id;
      leftNode.KeyIndex = leftNode.KeyCount - 1;
    }

    internal ulong TestEqualKeyData(Row key)
    {
      Row key1 = key.CopyInstance();
      key1.RowVersion = 0U;
      key1.RowId = 0U;
      Node node = this.GoKey(key1, (Node) null);
      Row currentKey = node[node.KeyIndex];
      bool isClustered = this.ParentIndex.IsClustered;
      bool flag1 = key.EqualColumns(currentKey, isClustered);
      if (!flag1 && node.KeyRank == Node.KeyPosition.OnRight)
      {
        this.GoNextKey();
        Node currentNode = this.CurrentNode;
        currentKey = currentNode[currentNode.KeyIndex];
        flag1 = key.EqualColumns(currentKey, isClustered);
      }
      uint transactionId1 = this.ParentIndex.TransactionId;
      bool flag2 = this.ParentIndex.PassTransaction(currentKey, transactionId1);
      if (flag1 && flag2)
        return (ulong) currentKey.RowId;
      for (; flag1 && !flag2; flag2 = this.ParentIndex.PassTransaction(currentKey, transactionId1))
      {
        uint transactionId2 = currentKey.TransactionId;
        if ((int) transactionId2 != (int) transactionId1 && (this.ParentIndex.DoGettingAnotherTransactionStatus(transactionId2) != TpStatus.Rollback && this.ParentIndex.PassTransaction(currentKey, transactionId2)))
          return (ulong) currentKey.RowId;
        this.GoNextKey();
        currentKey = this.CurrentKey;
        flag1 = (int) currentKey.RowId != (int) Row.MaxRowId && key.EqualColumns(currentKey, isClustered);
        if (!flag1)
          return (ulong) Row.MaxRowId;
      }
      return !flag1 || !flag2 ? (ulong) Row.MaxRowId : (ulong) currentKey.RowId;
    }

    internal Node GetNodeAtPosition(ulong position)
    {
      if ((long) position != (long) Row.EmptyReference)
        return this.OnGetNodeAtPosition(position);
      return (Node) null;
    }

    internal void RemoveNode(ulong id)
    {
      if ((long) this.currentNodeId == (long) id)
        this.currentNodeId = this.rootNodeId;
      this.Remove(id);
      this.nodeCache.Remove(id);
    }

    internal void SplitNode(Node node)
    {
      Node node1;
      int num1;
      if (node.IsRoot)
      {
        Node node2 = this.CreateNode(Node.NodeType.None);
        node.MoveKeysTo(node2);
        node2.Type = node.Type & Node.NodeType.Leaf;
        node2.RightNodeId = Row.EmptyReference;
        node2.LeftNodeId = Row.EmptyReference;
        node1 = node;
        node1.Type = Node.NodeType.Root;
        node1.Clear();
        node1.InsertKey(0, node2[0], node2.Id);
        node = node2;
        num1 = 0;
        Node node3 = this.nodeCache[node.Id];
      }
      else
      {
        node1 = this.GoKey(node[0], node);
        num1 = node1.KeyIndex;
      }
      Node node4 = this.CreateNode(Node.NodeType.None);
      int num2 = this.ParentIndex.DoSplitPolicy(node.KeyCount);
      int count = node.KeyCount - num2;
      node.MoveRightKeysTo(node4, count);
      node4.Type = node.Type & Node.NodeType.Leaf;
      node4.LeftNodeId = node.Id;
      node4.RightNodeId = node.RightNodeId;
      node.RightNodeId = node4.Id;
      Node nodeAtPosition = this.GetNodeAtPosition(node4.RightNodeId);
      node1.InsertKey(num1 + 1, node4[0], node4.Id);
      if (nodeAtPosition != null)
      {
        nodeAtPosition.LeftNodeId = node4.Id;
        nodeAtPosition.Flush(false);
        if (!nodeAtPosition.Modified)
          this.Remove(nodeAtPosition.Id);
      }
      node1.Flush(false);
      node4.Flush(false);
      node.Flush(false);
      if (!node.Modified)
        this.Remove(node.Id);
      if (!node4.Modified)
        this.Remove(node4.Id);
      if (!node1.Modified)
        this.Remove(node1.Id);
      node = this.nodeCache[node4.Id];
    }

    internal void AppendLeafKey(Row key)
    {
      this.AppendKey(key, this.GetNodeAtLevel(0));
    }

    internal void FinalizeAppending()
    {
      Node node = this.GetNodeAtLevel(0);
      for (int level = 1; level < this.parentNodeChain.Count; ++level)
      {
        node.Flush(true);
        this.Remove(node.Id);
        Row newKey = node[0].CopyInstance();
        newKey.RefPosition = node.Id;
        node = this.AppendKey(newKey, this.GetNodeAtLevel(level));
      }
      node.Type |= Node.NodeType.Root;
      this.rootNodeId = node.Id;
      node.KeyIndex = 0;
      node.Flush(true);
      this.Remove(node.Id);
      this.depth = this.parentNodeChain.Count;
      this.parentNodeChain.Clear();
    }

    public void Dispose()
    {
      if (this.isDisposed)
        return;
      if (this.cloneCounter > 0)
      {
        --this.cloneCounter;
      }
      else
      {
        this.isDisposed = true;
        GC.SuppressFinalize((object) this);
        foreach (Node node in this.Values)
          node.Dispose();
        this.parentIndex = (Index) null;
        this.Clear();
      }
    }
  }
}
