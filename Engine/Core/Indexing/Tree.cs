using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core.Indexing
{
    internal class Tree : Dictionary<ulong, Node>, IDisposable
    {
        private ulong rootNodeId = Row.EmptyReference;
        private ulong currentNodeId = Row.EmptyReference;
        private readonly NodeCache nodeCache = new NodeCache();
        private readonly List<Node> parentNodeChain = new List<Node>();
        private Index parentIndex;
        private readonly int pageSize;
        private bool modified;
        private int depth;
        private int cloneCounter;
        private bool isDisposed;

        internal Tree(Index parentIndex)
        {
            this.parentIndex = parentIndex;
            pageSize = parentIndex.PageSize;
        }

        internal Row CurrentKey
        {
            get
            {
                return GetCurrentKey();
            }
        }

        internal Node CurrentNode
        {
            get
            {
                return GetNodeAtPosition(currentNodeId);
            }
            set
            {
                currentNodeId = value.Id;
            }
        }

        internal Node RootNode
        {
            get
            {
                return GetNodeAtPosition(rootNodeId);
            }
        }

        internal bool Modified
        {
            get
            {
                return modified;
            }
            set
            {
                modified = value;
            }
        }

        internal Index ParentIndex
        {
            get
            {
                return parentIndex;
            }
        }

        internal new int Count
        {
            get
            {
                return base.Count + nodeCache.LiveReferenceCount;
            }
        }

        internal bool IsCacheEmpty
        {
            get
            {
                int num = base.Count + nodeCache.QueueCount;
                if (num == 0)
                    num = nodeCache.LiveReferenceCount;
                return num == 0;
            }
        }

        private Node CreateNodeInstance(ulong filePosition)
        {
            Node nodeInstance = OnCreateNodeInstance(filePosition, 0);
            nodeCache.AddToWeakCache(filePosition, nodeInstance);
            return nodeInstance;
        }

        private Node CreateNodeInstance(ulong filePosition, int levelIndex)
        {
            Node nodeInstance = OnCreateNodeInstance(filePosition, levelIndex);
            nodeCache.AddToWeakCache(filePosition, nodeInstance);
            return nodeInstance;
        }

        internal void GoRoot()
        {
            currentNodeId = rootNodeId;
            CurrentNode.KeyIndex = -1;
        }

        private Node FindNode(ulong nodeId)
        {
            Node node;
            if (!TryGetValue(nodeId, out node))
                node = nodeCache[nodeId];
            return node;
        }

        private Row GetCurrentKey()
        {
            Node currentNode = CurrentNode;
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
                node[keyIndex].FreeExtensionSpace(ParentIndex);
                CleanUpNodeSpace(node.GetChildNode(keyIndex), true);
            }
            if (!cleanCurrent)
                return;
            node.ClearApartment();
            RemoveNode(node.Id);
            node.Dispose();
        }

        internal void ModifiedNode(Node node)
        {
            modified = true;
            if (ContainsKey(node.Id) || node.IsDisposed)
                return;
            Add(node.Id, node);
        }

        private Node GetNodeAtLevel(int level)
        {
            if (level < parentNodeChain.Count)
                return parentNodeChain[level];
            if (level != parentNodeChain.Count)
                return null;
            Node node = CreateNode(level == 0 ? Node.NodeType.Leaf : Node.NodeType.None, level);
            parentNodeChain.Add(node);
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
                Node node = CreateNode(nodeType, treeLevel);
                parentNodeChain[treeLevel] = node;
                try
                {
                    node.LeftNodeId = levelNode.Id;
                    levelNode.RightNodeId = node.Id;
                    levelNode.Type = nodeType;
                    Row newKey1 = levelNode[0].CopyInstance();
                    newKey1.RefPosition = levelNode.Id;
                    levelNode.Flush(true);
                    RemoveNode(levelNode.Id);
                    AppendKey(newKey1, GetNodeAtLevel(treeLevel + 1));
                }
                finally
                {
                    levelNode.Dispose();
                }
                AppendKey(newKey, node);
                return node;
            }
            levelNode.Add(newKey);
            levelNode.KeyCount = levelNode.Count;
            return levelNode;
        }

        protected virtual Node OnCreateNodeInstance(ulong filePosition, int level)
        {
            return Node.CreateInstance(filePosition, level, this, parentIndex.TopRow, false, parentIndex.PageSize);
        }

        protected virtual Node OnGetNodeAtPosition(ulong filePosition)
        {
            Node node1 = FindNode(filePosition);
            if (node1 == null)
            {
                node1 = CreateNodeInstance(filePosition);
                try
                {
                    node1.Update();
                }
                catch
                {
                    throw;
                }
                if (!ContainsKey(filePosition))
                {
                    Node node2 = nodeCache[filePosition];
                }
            }
            return node1;
        }

        public new void Clear()
        {
            modified = false;
            depth = 0;
            rootNodeId = Row.EmptyReference;
            currentNodeId = Row.EmptyReference;
            base.Clear();
            nodeCache.Clear();
            parentNodeChain.Clear();
        }

        internal Tree GetClone()
        {
            ++cloneCounter;
            return this;
        }

        internal void FlushTree()
        {
            if (!modified && base.Count == 0)
                return;
            while (base.Count > 0)
            {
                foreach (Node node in new List<Node>(Values))
                {
                    if (node.Modified)
                        node.Flush(false);
                    if (!node.Modified)
                        Remove(node.Id);
                }
            }
            base.Clear();
            modified = false;
        }

        internal bool MinimizeTreeMemory(bool forceClearing)
        {
            int num = 10;
            if (forceClearing)
                num = 1;
            if (base.Count <= num)
                return true;
            FlushTree();
            return true;
        }

        internal void ActivateRoot(ulong rootPosition)
        {
            rootNodeId = rootPosition;
            Node nodeAtPosition = GetNodeAtPosition(rootPosition);
            if (nodeAtPosition == null)
                throw new VistaDBException(139, rootPosition.ToString());
            currentNodeId = rootPosition;
            nodeAtPosition.KeyIndex = 0;
        }

        internal void CreateRoot()
        {
            Node node = CreateNode(Node.NodeType.Root | Node.NodeType.Leaf);
            rootNodeId = node.Id;
            parentNodeChain.Add(node);
            currentNodeId = rootNodeId;
            node.KeyIndex = 0;
        }

        internal Node CreateNode(Node.NodeType nodeType, int levelIndex)
        {
            ulong freeCluster = parentIndex.GetFreeCluster(1);
            Node nodeInstance = CreateNodeInstance(freeCluster, levelIndex);
            Add(freeCluster, nodeInstance);
            currentNodeId = freeCluster;
            nodeInstance.Type = nodeType;
            return nodeInstance;
        }

        internal Node CreateNode(Node.NodeType nodeType)
        {
            ulong freeCluster = parentIndex.GetFreeCluster(1);
            Node nodeInstance = CreateNodeInstance(freeCluster);
            Add(freeCluster, nodeInstance);
            currentNodeId = freeCluster;
            nodeInstance.Type = nodeType;
            return nodeInstance;
        }

        internal bool ReplaceKey(Row oldKey, Row newKey, uint transactionId)
        {
            Node node1;
            bool flag;
            int index;
            if ((int)oldKey.RowId != (int)Row.MinRowId && (int)oldKey.RowId != (int)Row.MaxRowId)
            {
                node1 = GoKey(oldKey, null);
                flag = node1.KeyRank == Node.KeyPosition.Equal;
                index = node1.KeyIndex;
                if (transactionId != 0U && flag && ((int)oldKey.TransactionId != (int)transactionId || oldKey.OutdatedStatus))
                {
                    Row newKey1 = oldKey.CopyInstance();
                    newKey1.RowVersion = transactionId;
                    newKey1.OutdatedStatus = true;
                    ReplaceKey(oldKey, newKey1, 0U);
                    flag = false;
                }
            }
            else
            {
                node1 = null;
                flag = false;
                index = -1;
            }
            Node node2 = GoKey(newKey, null);
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
                currentNodeId = node2.Id;
                node2.KeyIndex = -1;
            }
        }

        internal bool DeleteKey(Row oldKey, uint transactionId)
        {
            Node node = GoKey(oldKey, null);
            if (node.KeyRank != Node.KeyPosition.Equal)
                return false;
            if (transactionId != 0U && ((int)oldKey.TransactionId != (int)transactionId || oldKey.OutdatedStatus))
            {
                Row newKey = oldKey.CopyInstance();
                newKey.RowVersion = transactionId;
                newKey.OutdatedStatus = true;
                ReplaceKey(oldKey, newKey, 0U);
                if (node.KeyCount > 0)
                    currentNodeId = node.Id;
                ++node.KeyIndex;
            }
            else
            {
                node = node.DeleteKey(node.KeyIndex);
                currentNodeId = node.Id;
            }
            if (node.KeyIndex == node.KeyCount)
                GoNextKey();
            return true;
        }

        internal Node GoKey(Row key, Node lowestNode)
        {
            int num = 0;
            Node node1 = RootNode;
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
                depth = num;
            currentNodeId = node2.Id;
            return node2;
        }

        internal void GoNextKey()
        {
            Node currentNode = CurrentNode;
            ++currentNode.KeyIndex;
            if (currentNode.KeyIndex < currentNode.Count)
                return;
            currentNode.KeyIndex = currentNode.Count;
            Node rightNode = currentNode.GetRightNode();
            if (rightNode == null)
                return;
            currentNodeId = rightNode.Id;
            rightNode.KeyIndex = 0;
        }

        internal void GoPrevKey()
        {
            Node currentNode = CurrentNode;
            --currentNode.KeyIndex;
            if (currentNode.KeyIndex >= 0)
                return;
            currentNode.KeyIndex = -1;
            Node leftNode = currentNode.GetLeftNode();
            if (leftNode == null)
                return;
            currentNodeId = leftNode.Id;
            leftNode.KeyIndex = leftNode.KeyCount - 1;
        }

        internal ulong TestEqualKeyData(Row key)
        {
            Row key1 = key.CopyInstance();
            key1.RowVersion = 0U;
            key1.RowId = 0U;
            Node node = GoKey(key1, null);
            Row currentKey = node[node.KeyIndex];
            bool isClustered = ParentIndex.IsClustered;
            bool flag1 = key.EqualColumns(currentKey, isClustered);
            if (!flag1 && node.KeyRank == Node.KeyPosition.OnRight)
            {
                GoNextKey();
                Node currentNode = CurrentNode;
                currentKey = currentNode[currentNode.KeyIndex];
                flag1 = key.EqualColumns(currentKey, isClustered);
            }
            uint transactionId1 = ParentIndex.TransactionId;
            bool flag2 = ParentIndex.PassTransaction(currentKey, transactionId1);
            if (flag1 && flag2)
                return currentKey.RowId;
            for (; flag1 && !flag2; flag2 = ParentIndex.PassTransaction(currentKey, transactionId1))
            {
                uint transactionId2 = currentKey.TransactionId;
                if ((int)transactionId2 != (int)transactionId1 && (ParentIndex.DoGettingAnotherTransactionStatus(transactionId2) != TpStatus.Rollback && ParentIndex.PassTransaction(currentKey, transactionId2)))
                    return currentKey.RowId;
                GoNextKey();
                currentKey = CurrentKey;
                flag1 = (int)currentKey.RowId != (int)Row.MaxRowId && key.EqualColumns(currentKey, isClustered);
                if (!flag1)
                    return Row.MaxRowId;
            }
            return !flag1 || !flag2 ? Row.MaxRowId : currentKey.RowId;
        }

        internal Node GetNodeAtPosition(ulong position)
        {
            if ((long)position != (long)Row.EmptyReference)
                return OnGetNodeAtPosition(position);
            return null;
        }

        internal void RemoveNode(ulong id)
        {
            if ((long)currentNodeId == (long)id)
                currentNodeId = rootNodeId;
            Remove(id);
            nodeCache.Remove(id);
        }

        internal void SplitNode(Node node)
        {
            Node node1;
            int num1;
            if (node.IsRoot)
            {
                Node node2 = CreateNode(Node.NodeType.None);
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
                Node node3 = nodeCache[node.Id];
            }
            else
            {
                node1 = GoKey(node[0], node);
                num1 = node1.KeyIndex;
            }
            Node node4 = CreateNode(Node.NodeType.None);
            int num2 = ParentIndex.DoSplitPolicy(node.KeyCount);
            int count = node.KeyCount - num2;
            node.MoveRightKeysTo(node4, count);
            node4.Type = node.Type & Node.NodeType.Leaf;
            node4.LeftNodeId = node.Id;
            node4.RightNodeId = node.RightNodeId;
            node.RightNodeId = node4.Id;
            Node nodeAtPosition = GetNodeAtPosition(node4.RightNodeId);
            node1.InsertKey(num1 + 1, node4[0], node4.Id);
            if (nodeAtPosition != null)
            {
                nodeAtPosition.LeftNodeId = node4.Id;
                nodeAtPosition.Flush(false);
                if (!nodeAtPosition.Modified)
                    Remove(nodeAtPosition.Id);
            }
            node1.Flush(false);
            node4.Flush(false);
            node.Flush(false);
            if (!node.Modified)
                Remove(node.Id);
            if (!node4.Modified)
                Remove(node4.Id);
            if (!node1.Modified)
                Remove(node1.Id);
            node = nodeCache[node4.Id];
        }

        internal void AppendLeafKey(Row key)
        {
            AppendKey(key, GetNodeAtLevel(0));
        }

        internal void FinalizeAppending()
        {
            Node node = GetNodeAtLevel(0);
            for (int level = 1; level < parentNodeChain.Count; ++level)
            {
                node.Flush(true);
                Remove(node.Id);
                Row newKey = node[0].CopyInstance();
                newKey.RefPosition = node.Id;
                node = AppendKey(newKey, GetNodeAtLevel(level));
            }
            node.Type |= Node.NodeType.Root;
            rootNodeId = node.Id;
            node.KeyIndex = 0;
            node.Flush(true);
            Remove(node.Id);
            depth = parentNodeChain.Count;
            parentNodeChain.Clear();
        }

        public void Dispose()
        {
            if (isDisposed)
                return;
            if (cloneCounter > 0)
            {
                --cloneCounter;
            }
            else
            {
                isDisposed = true;
                GC.SuppressFinalize(this);
                foreach (Node node in Values)
                    node.Dispose();
                parentIndex = null;
                Clear();
            }
        }
    }
}
